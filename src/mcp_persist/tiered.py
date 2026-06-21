"""Tiered hot/cold event store chaining for archive-and-resume."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventMessage,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage

from mcp_persist.metrics import NoOpMetricsCollector, safe_call

if TYPE_CHECKING:
    from mcp_persist.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class ChainedEventStore(EventStore):
    """EventStore that writes to a hot backend and falls back to cold on replay miss.

    New events go to ``hot`` only. :meth:`replay_events_after` resolves the anchor
    in ``hot`` first; when the anchor is absent from ``hot`` but present in
    ``cold`` (typical after archival), events are replayed from ``cold`` and then
    continued from ``hot`` on the same ``stream_id`` in monotonic ``event_id``
    order.

    Args:
        hot:  The primary store (recent events, usually with a ttl).
        cold: The archive store (``ttl=None`` recommended; ID-preserving inserts).
        metrics: Optional collector; when omitted a no-op is used.
    """

    def __init__(
        self,
        hot: EventStore,
        cold: EventStore,
        *,
        metrics: MetricsCollector | None = None,
    ) -> None:
        self._hot = hot
        self._cold = cold
        self._metrics: MetricsCollector = metrics if metrics is not None else NoOpMetricsCollector()

    async def store_event(
        self,
        stream_id: StreamId,
        message: JSONRPCMessage | None,
    ) -> EventId:
        if type(self._metrics) is NoOpMetricsCollector:
            return await self._hot.store_event(stream_id, message)
        start = time.monotonic()
        try:
            event_id = await self._hot.store_event(stream_id, message)
        except Exception as exc:
            safe_call(self._metrics.on_error, "store_event", exc)
            raise
        safe_call(self._metrics.on_store_event, stream_id, event_id, (time.monotonic() - start) * 1000.0)
        return event_id

    async def replay_events_after(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> StreamId | None:
        if type(self._metrics) is NoOpMetricsCollector:
            return await self._replay_events_after_impl(last_event_id, send_callback)
        start = time.monotonic()
        count = 0

        async def counting_callback(event: EventMessage) -> None:
            nonlocal count
            count += 1
            await send_callback(event)

        try:
            stream_id = await self._replay_events_after_impl(last_event_id, counting_callback)
        except Exception as exc:
            safe_call(self._metrics.on_error, "replay_events_after", exc)
            raise
        safe_call(self._metrics.on_replay, stream_id, count, (time.monotonic() - start) * 1000.0)
        return stream_id

    async def _replay_events_after_impl(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> StreamId | None:
        in_hot = await self._event_exists(self._hot, last_event_id)
        in_cold = await self._event_exists(self._cold, last_event_id)

        if not in_hot and not in_cold:
            return None

        if in_hot:
            return await self._hot.replay_events_after(last_event_id, send_callback)

        stream_id = await self._stream_id_for_event(self._cold, last_event_id)
        if stream_id is None:
            return None

        last_replayed = int(last_event_id)

        async def cold_callback(event: EventMessage) -> None:
            nonlocal last_replayed
            if event.event_id is not None:
                last_replayed = int(event.event_id)
            await send_callback(event)

        await self._cold.replay_events_after(last_event_id, cold_callback)
        await self._replay_stream_after(self._hot, stream_id, last_replayed, send_callback)
        return stream_id

    @staticmethod
    async def _event_exists(store: Any, event_id: EventId) -> bool:
        exists: Any = getattr(store, "_event_exists", None)
        if exists is not None:
            return await exists(event_id)
        return False

    @staticmethod
    async def _stream_id_for_event(store: Any, event_id: EventId) -> StreamId | None:
        lookup: Any = getattr(store, "_stream_id_for_event", None)
        if lookup is not None:
            return await lookup(event_id)
        return None

    @staticmethod
    async def _replay_stream_after(
        store: Any,
        stream_id: StreamId,
        after_event_id: int,
        send_callback: EventCallback,
    ) -> None:
        iter_events: Any = getattr(store, "_iter_stream_events", None)
        if iter_events is None:
            return

        async for event_id, message in iter_events(stream_id):
            if int(event_id) <= after_event_id:
                continue
            if message is None:
                continue
            await send_callback(EventMessage(message=message, event_id=event_id))
