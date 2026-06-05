"""A single upstream SSE stream, buffered and stored for resumable fan-out.

A :class:`StreamBuffer` owns one upstream Server-Sent Events response. It runs a
background task that parses each frame, persists it to an :class:`EventStore`
(which assigns the proxy's own monotonic event ID), and appends it to a bounded
in-memory deque. Any number of consumers can then read the stream via
:meth:`consume_from`, each from its own cursor, and each receives every event
from that cursor onward — replayed from the store if it predates the live
window, or served from the deque (and live, as frames arrive) if it does not.

The buffer outlives the request handler that created it: a client can disconnect
and the background task keeps consuming and storing, so a later reconnect finds
a complete history.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Protocol

from mcp.server.streamable_http import EventId, EventMessage, EventStore
from mcp.types import JSONRPCMessage
from pydantic import TypeAdapter

from mcp_persist._sse_parser import SSEFrame, SSEParser

logger = logging.getLogger(__name__)

_jsonrpc_message_adapter: TypeAdapter[JSONRPCMessage] = TypeAdapter(JSONRPCMessage)

# Live-tail window. Reconnects whose Last-Event-ID predates the window fall back
# to store replay, so this only bounds memory, not correctness.
DEFAULT_DEQUE_MAXLEN = 1024


class SSEByteStream(Protocol):
    """The slice of ``httpx.Response`` the buffer consumes.

    Declared structurally so the buffer carries no ``httpx`` import (it only ever
    *consumes* a response, never builds one) and tests can pass a lightweight
    fake. ``httpx.Response`` satisfies this.
    """

    def aiter_text(self) -> AsyncIterator[str]: ...

    async def aclose(self) -> None: ...


class StreamBuffer:
    """Consumes one upstream SSE stream; stores, buffers, and fans out events."""

    def __init__(self, stream_id: str, store: EventStore, *, maxlen: int = DEFAULT_DEQUE_MAXLEN) -> None:
        self.stream_id = stream_id
        self.store = store
        self.done = False
        self._deque: deque[tuple[EventId, str]] = deque(maxlen=maxlen)
        self._waiters: list[asyncio.Future[None]] = []
        self._task: asyncio.Task[None] | None = None
        self._response: SSEByteStream | None = None
        # The first exception raised by _consume, surfaced to consumers rather
        # than swallowed as a silent truncation.
        self._exc: Exception | None = None

    def start(self, response: SSEByteStream) -> None:
        """Begin consuming ``response`` in a background task.

        The task owns ``response`` and closes it in its ``finally``; do not wrap
        the response in an ``async with`` in the caller, or it will be torn out
        from under the still-running task when the caller returns.
        """
        if self._task is not None:
            raise RuntimeError("StreamBuffer already started")
        self._response = response
        self._task = asyncio.create_task(self._consume(response))

    async def aclose(self) -> None:
        """Cancel the consume task and wait for it to finish (idempotent).

        Closes the upstream response (via ``_consume``'s ``finally``) and wakes
        any parked consumers. Safe to call before :meth:`start` or twice.
        """
        if self._task is None:
            return
        if not self._task.done():
            self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass

    # ── producer side ──────────────────────────────────────────────────────

    async def _consume(self, response: SSEByteStream) -> None:
        parser = SSEParser()
        try:
            async for chunk in response.aiter_text():
                for frame in parser.feed(chunk):
                    await self._ingest(frame)
            for frame in parser.flush():
                await self._ingest(frame)
        except Exception as exc:  # noqa: BLE001 - capture any upstream/parse/store failure so a consumer can re-raise it
            self._exc = exc
        finally:
            # Signal completion and wake consumers *before* closing the response:
            # done + notify are synchronous and must run even if aclose() is
            # cancelled or raises, or a parked consumer would hang.
            self.done = True
            self._notify()
            try:
                await response.aclose()
            except Exception:  # noqa: BLE001 - best-effort close; never mask the stream's real outcome
                logger.exception("error closing upstream SSE response for stream %s", self.stream_id)

    async def _ingest(self, frame: SSEFrame) -> None:
        if frame.data:
            try:
                message: JSONRPCMessage | None = _jsonrpc_message_adapter.validate_json(frame.data)
            except ValueError:
                # The EventStore can only hold JSON-RPC messages, so a frame we
                # cannot parse cannot be persisted or re-IDed. Skip it rather
                # than abort the stream — mirrors the store's own skip-corrupt
                # behaviour on replay.
                logger.warning("skipping unparseable SSE frame on stream %s", self.stream_id)
                return
        else:
            message = None  # priming event
        event_id = await self.store.store_event(self.stream_id, message)
        self._deque.append((event_id, frame.data))
        self._notify()

    def _notify(self) -> None:
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._waiters.clear()

    # ── consumer side ──────────────────────────────────────────────────────

    async def consume_from(self, after: EventId | None) -> AsyncGenerator[tuple[EventId, str], None]:
        """Yield ``(event_id, data)`` from ``after`` onward, blocking for live events.

        ``after`` is the consumer's last-seen event ID, or ``None`` to read from
        the start of the live window (a fresh stream, where the buffer was
        created with the upstream connection so the window holds everything so
        far). Each call maintains its own cursor, so multiple consumers can read
        the same buffer concurrently.

        Cold vs. hot is re-evaluated every iteration: if a fast producer evicts
        events past the live window mid-read, the next pass replays the gap from
        the store and resumes from the window once caught up.

        Raises:
            Exception: whatever :meth:`_consume` captured, once all buffered
                events have been delivered — the upstream stream failed.
        """
        cursor = self._normalize_cursor(after)
        while True:
            # COLD: the store is authoritative for everything after the cursor
            # that the live window no longer covers. Replay yields parsed
            # messages (priming events excluded), re-serialized the way the store
            # wrote them; the original wire bytes are not recoverable here.
            if self._cold_needed(cursor):
                assert cursor is not None  # _cold_needed is False for a None cursor
                replayed: list[tuple[EventId, str]] = []

                async def _collect(event: EventMessage) -> None:
                    assert event.event_id is not None
                    data = event.message.model_dump_json(by_alias=True, exclude_none=True)
                    replayed.append((event.event_id, data))  # noqa: B023 - consumed before the next loop iteration

                await self.store.replay_events_after(cursor, _collect)
                if replayed:
                    for event_id, data in replayed:
                        yield event_id, data
                        cursor = event_id
                    continue
                # Store is caught up; fall through to the live window / park.

            # HOT: every event after the cursor is in the deque. Iterate a
            # snapshot so a concurrent append can't disturb iteration. From the
            # snapshot read to the waiter registration there is no await/yield
            # (when nothing is delivered), so a producer cannot append-and-notify
            # unseen — no lost wakeup.
            progressed = False
            for event_id, data in list(self._deque):
                if cursor is None or int(event_id) > int(cursor):
                    yield event_id, data
                    cursor = event_id
                    progressed = True
            if progressed:
                continue
            if self.done:
                if self._exc is not None:
                    raise self._exc
                return
            waiter = asyncio.get_running_loop().create_future()
            self._waiters.append(waiter)
            await waiter

    def _normalize_cursor(self, after: EventId | None) -> EventId | None:
        """Coerce a client-supplied Last-Event-ID into something comparable.

        Event IDs are decimal strings across every backend. A non-numeric value
        is malformed (the header is client-controlled), so we resume from the
        live tail rather than raise mid-stream or redeliver the whole window.
        """
        if after is None or after.isdigit():
            return after
        logger.warning("non-numeric Last-Event-ID %r on stream %s; resuming from live tail", after, self.stream_id)
        return self._deque[-1][0] if self._deque else None

    def _cold_needed(self, cursor: EventId | None) -> bool:
        if cursor is None:
            return False  # fresh stream: read the live window from its oldest entry
        if not self._deque:
            return True  # have a cursor but no window loaded yet; the store may hold history
        return int(cursor) < int(self._deque[0][0])
