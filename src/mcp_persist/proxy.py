"""An ASGI proxy that adds SSE resumability to any upstream MCP server.

``PersistenceProxy`` sits in front of an MCP server's streamable-HTTP endpoint.
It forwards requests upstream, and for ``text/event-stream`` responses it
intercepts the SSE stream: each event is parsed, persisted to an
:class:`~mcp.server.streamable_http.EventStore` (which assigns the proxy's own
monotonic event ID), and forwarded to the client. A client that disconnects can
reconnect with ``Last-Event-ID`` and the proxy replays from the store, then
continues live — without the upstream needing an event store of its own.

What it does and does not do:

* It adds resumability against a **stable upstream**. If the client and the
  proxy both drop before an event is stored, that event is gone; if the upstream
  itself restarts (new session, new IDs), buffered history can still be replayed
  but new events arrive on a new stream.
* The upstream runs **without** its own event store — the proxy is the store. A
  storeless upstream emits no priming events and no event IDs; the proxy assigns
  them.

Usage::

    async with PersistenceProxy.create(
        upstream="http://localhost:8001", backend="sqlite", url="events.db", ttl=3600,
    ) as proxy:
        uvicorn.run(proxy, port=8000)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import httpx

from mcp_persist._stream_buffer import DEFAULT_DEQUE_MAXLEN, StreamBuffer
from mcp_persist.config import event_store_from_env
from mcp_persist.postgres import PostgresEventStore
from mcp_persist.redis import RedisEventStore
from mcp_persist.sqlite import SQLiteEventStore

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from mcp.server.streamable_http import EventId, EventStore

logger = logging.getLogger(__name__)

# Mirrors the SDK's standalone-GET stream key (streamable_http.GET_STREAM_KEY).
GET_STREAM_KEY = "_GET_stream"

# Hop-by-hop headers (RFC 7230 §6.1) plus framing headers httpx/the ASGI server
# must compute themselves — never forwarded verbatim in either direction.
_SKIP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
    }
)

Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]


class PersistenceProxy:
    """ASGI app fronting an upstream MCP server with SSE resumability."""

    def __init__(
        self,
        upstream: str,
        store: EventStore,
        *,
        mcp_path: str = "/mcp",
        client: httpx.AsyncClient | None = None,
        buffer_grace_ttl: float = 60.0,
        buffer_maxlen: int = DEFAULT_DEQUE_MAXLEN,
        timeout: float = 300.0,
    ) -> None:
        self._upstream_base = upstream.rstrip("/")
        self._mcp_path = mcp_path
        self._upstream_url = self._upstream_base + mcp_path
        self._store = store
        self._buffer_grace_ttl = buffer_grace_ttl
        self._buffer_maxlen = buffer_maxlen
        # No read timeout: SSE streams are idle for long stretches. Follow
        # redirects so an upstream's trailing-slash redirect (/mcp -> /mcp/) is
        # resolved here rather than handed to the client, which would otherwise
        # follow it straight to the upstream and bypass the proxy.
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(timeout, read=None), follow_redirects=True)
        self._buffers: dict[str, StreamBuffer] = {}

    @classmethod
    @asynccontextmanager
    async def create(
        cls,
        upstream: str,
        *,
        store: EventStore | None = None,
        backend: str | None = None,
        url: str | None = None,
        ttl: int | None = None,
        mcp_path: str = "/mcp",
        buffer_grace_ttl: float = 60.0,
        timeout: float = 300.0,
    ) -> AsyncIterator[PersistenceProxy]:
        """Build a proxy, resolving the store the same way as ``with_persistence``.

        The store is chosen by the first that is set: ``store=`` (caller-owned,
        not closed here), ``backend=``+``url=`` (built and closed on exit), or
        neither (``MCP_PERSIST_*`` environment variables).
        """
        ctx, owned = _resolve_store(store, backend=backend, url=url, ttl=ttl)
        client = httpx.AsyncClient(timeout=httpx.Timeout(timeout, read=None), follow_redirects=True)
        proxy: PersistenceProxy | None = None
        try:
            if ctx is not None:
                async with ctx as resolved:
                    proxy = cls(upstream, resolved, mcp_path=mcp_path, client=client, buffer_grace_ttl=buffer_grace_ttl)
                    yield proxy
            else:
                assert owned is not None
                proxy = cls(upstream, owned, mcp_path=mcp_path, client=client, buffer_grace_ttl=buffer_grace_ttl)
                yield proxy
        finally:
            if proxy is not None:
                await proxy.close_all()
            else:
                await client.aclose()

    async def close_all(self) -> None:
        """Cancel every active buffer task and close the HTTP client."""
        buffers = list(self._buffers.values())
        self._buffers.clear()
        for buf in buffers:
            await buf.aclose()
        await self._client.aclose()

    # ── ASGI entry point ─────────────────────────────────────────────────────

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self._handle_lifespan(receive, send)
            return
        if scope["type"] != "http":  # pragma: no cover - websockets etc. unsupported
            return
        path, method = scope["path"], scope["method"]
        if path == self._mcp_path and method == "POST":
            await self._handle_post(scope, receive, send)
        elif path == self._mcp_path and method == "GET":
            await self._handle_get(scope, receive, send)
        else:
            await self._passthrough(scope, receive, send)

    async def _handle_lifespan(self, receive: Receive, send: Send) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await self.close_all()
                await send({"type": "lifespan.shutdown.complete"})
                return

    # ── POST ─────────────────────────────────────────────────────────────────

    async def _handle_post(self, scope: Scope, receive: Receive, send: Send) -> None:
        body = await _read_body(receive)
        request_id = _extract_request_id(body)

        request = self._client.build_request(
            "POST", self._upstream_url, content=body, headers=_forward_headers(scope["headers"])
        )
        response = await self._client.send(request, stream=True)

        if "text/event-stream" not in response.headers.get("content-type", "").lower():
            # Plain JSON (or error) response: forward verbatim, store nothing.
            try:
                await _forward_response(response, send)
            finally:
                await response.aclose()
            return

        # SSE response: intercept. Prefer the session id the upstream assigned
        # (on initialize); otherwise the one the client already holds.
        session_id = response.headers.get("mcp-session-id") or _header(scope, b"mcp-session-id") or uuid.uuid4().hex
        stream_key = request_id if request_id is not None else uuid.uuid4().hex
        stream_id = f"{session_id}:{stream_key}"

        buf = StreamBuffer(stream_id, self._store, maxlen=self._buffer_maxlen)
        self._register_buffer(stream_id, buf)
        buf.start(response)  # the buffer now owns the response; do not aclose it here

        await _send_sse_start(send, session_id)
        await _stream_to_client(buf.consume_from(after=None), receive, send)

    # ── GET ──────────────────────────────────────────────────────────────────

    async def _handle_get(self, scope: Scope, receive: Receive, send: Send) -> None:
        session_id = _header(scope, b"mcp-session-id") or uuid.uuid4().hex
        last_event_id = _header(scope, b"last-event-id")
        get_stream_id = f"{session_id}:{GET_STREAM_KEY}"
        live = self._buffers.get(get_stream_id)

        if last_event_id is not None:
            if live is not None and not live.done:
                # Live GET stream: replay the gap from the store, then continue live.
                gen = live.consume_from(after=last_event_id)
            else:
                # No live GET buffer: replay from the store (the event id resolves
                # its owning stream — a completed POST stream or an expired GET
                # buffer), then resume notifications from a fresh upstream GET.
                gen = self._replay_then_resume_get(session_id, last_event_id, scope)
        else:
            if live is not None and not live.done:
                # Reuse the live buffer instead of opening a second upstream GET
                # (the SDK allows only one standalone GET stream per session).
                gen = live.consume_from(after=None)
            else:
                gen = self._fresh_get(session_id, scope)

        await _send_sse_start(send, session_id)
        await _stream_to_client(gen, receive, send)

    async def _fresh_get(self, session_id: str, scope: Scope) -> AsyncGenerator[tuple[EventId, str], None]:
        buf = await self._open_get_buffer(session_id, scope)
        async for item in buf.consume_from(after=None):
            yield item

    async def _replay_then_resume_get(
        self, session_id: str, last_event_id: EventId, scope: Scope
    ) -> AsyncGenerator[tuple[EventId, str], None]:
        async for item in _store_replay(self._store, last_event_id):
            yield item
        buf = await self._open_get_buffer(session_id, scope)
        async for item in buf.consume_from(after=None):
            yield item

    async def _open_get_buffer(self, session_id: str, scope: Scope) -> StreamBuffer:
        get_stream_id = f"{session_id}:{GET_STREAM_KEY}"
        request = self._client.build_request(
            "GET", self._upstream_url, headers=_forward_headers(scope["headers"], strip={"last-event-id"})
        )
        response = await self._client.send(request, stream=True)  # the buffer owns this response
        buf = StreamBuffer(get_stream_id, self._store, maxlen=self._buffer_maxlen)
        self._register_buffer(get_stream_id, buf)
        buf.start(response)
        return buf

    # ── passthrough (non-MCP paths, DELETE, etc.) ─────────────────────────────

    async def _passthrough(self, scope: Scope, receive: Receive, send: Send) -> None:
        body = await _read_body(receive)
        url = self._upstream_base + scope["path"]
        if scope.get("query_string"):
            url += "?" + scope["query_string"].decode("latin-1")
        request = self._client.build_request(
            scope["method"], url, content=body or None, headers=_forward_headers(scope["headers"])
        )
        response = await self._client.send(request, stream=True)
        try:
            await _forward_response(response, send)
        finally:
            await response.aclose()

    # ── buffer registry (a plain dict + grace-period cleanup) ─────────────────

    def _register_buffer(self, stream_id: str, buf: StreamBuffer) -> None:
        self._buffers[stream_id] = buf
        task = buf._task
        if task is not None:
            task.add_done_callback(lambda _t: self._schedule_eviction(stream_id, buf))

    def _schedule_eviction(self, stream_id: str, buf: StreamBuffer) -> None:
        # Keep a finished buffer around briefly so an immediate reconnect still
        # finds it live; after that the store holds the history.
        with contextlib.suppress(RuntimeError):  # loop already closed during shutdown
            asyncio.get_running_loop().call_later(self._buffer_grace_ttl, self._evict, stream_id, buf)

    def _evict(self, stream_id: str, buf: StreamBuffer) -> None:
        if self._buffers.get(stream_id) is buf:
            del self._buffers[stream_id]


# ── module-level helpers ─────────────────────────────────────────────────────


async def _store_replay(store: EventStore, after: EventId) -> AsyncGenerator[tuple[EventId, str], None]:
    """Pure store replay (no live upstream), via a done buffer with an empty window."""
    buf = StreamBuffer(f"replay:{uuid.uuid4().hex}", store)
    buf.done = True
    async for item in buf.consume_from(after):
        yield item


async def _read_body(receive: Receive) -> bytes:
    chunks: list[bytes] = []
    while True:
        message = await receive()
        if message["type"] == "http.request":
            chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        elif message["type"] == "http.disconnect":  # pragma: no cover - client gone before body sent
            break
    return b"".join(chunks)


async def _watch_disconnect(receive: Receive) -> None:
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return


def _header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope["headers"]:
        if key == name:
            return value.decode("latin-1")
    return None


def _forward_headers(
    raw: Iterable[tuple[bytes, bytes]], strip: frozenset[str] | set[str] = frozenset()
) -> dict[str, str]:
    out: dict[str, str] = {}
    for name, value in raw:
        key = name.decode("latin-1").lower()
        if key in _SKIP_HEADERS or key in strip:
            continue
        out[key] = value.decode("latin-1")
    return out


def _extract_request_id(body: bytes) -> str | None:
    try:
        obj = json.loads(body)
    except (json.JSONDecodeError, ValueError):
        return None
    if isinstance(obj, dict):
        rid = obj.get("id")
        return None if rid is None else str(rid)
    return None


def _response_headers(response: httpx.Response, body_len: int) -> list[tuple[bytes, bytes]]:
    headers: list[tuple[bytes, bytes]] = []
    for key, value in response.headers.multi_items():
        if key.lower() in _SKIP_HEADERS:
            continue
        headers.append((key.lower().encode("latin-1"), value.encode("latin-1")))
    headers.append((b"content-length", str(body_len).encode()))
    return headers


async def _forward_response(response: httpx.Response, send: Send) -> None:
    body = await response.aread()
    await send(
        {
            "type": "http.response.start",
            "status": response.status_code,
            "headers": _response_headers(response, len(body)),
        }
    )
    await send({"type": "http.response.body", "body": body, "more_body": False})


async def _send_sse_start(send: Send, session_id: str | None) -> None:
    headers: list[tuple[bytes, bytes]] = [
        (b"content-type", b"text/event-stream"),
        (b"cache-control", b"no-cache, no-transform"),
        (b"connection", b"keep-alive"),
    ]
    if session_id:
        headers.append((b"mcp-session-id", session_id.encode("latin-1")))
    await send({"type": "http.response.start", "status": 200, "headers": headers})


def _sse_frame(event_id: EventId, data: str) -> bytes:
    if data:
        return f"id: {event_id}\r\nevent: message\r\ndata: {data}\r\n\r\n".encode()
    return f"id: {event_id}\r\ndata: \r\n\r\n".encode()  # priming event


async def _pump(gen: AsyncGenerator[tuple[EventId, str], None], send: Send) -> None:
    async for event_id, data in gen:
        await send({"type": "http.response.body", "body": _sse_frame(event_id, data), "more_body": True})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


async def _stream_to_client(gen: AsyncGenerator[tuple[EventId, str], None], receive: Receive, send: Send) -> None:
    """Pump ``gen`` to the client; stop when it ends or the client disconnects.

    Disconnect is watched in a sibling task and cancels the pump — polling
    between events would miss a disconnect during an idle SSE stream. The
    generator (and so the buffer's ``consume_from``) is always closed; the
    buffer's own background task is unaffected and keeps storing.
    """
    pump = asyncio.create_task(_pump(gen, send))
    watch = asyncio.create_task(_watch_disconnect(receive))
    try:
        await asyncio.wait({pump, watch}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        watch.cancel()
        if not pump.done():
            pump.cancel()
        results = await asyncio.gather(pump, watch, return_exceptions=True)
        await gen.aclose()
    pump_result = results[0]
    if isinstance(pump_result, BaseException) and not isinstance(pump_result, asyncio.CancelledError):
        logger.warning("error while streaming to client: %r", pump_result)


def _resolve_store(
    store: EventStore | None,
    *,
    backend: str | None,
    url: str | None,
    ttl: int | None,
) -> tuple[AbstractAsyncContextManager[EventStore] | None, EventStore | None]:
    """Resolve into ``(ctx, store)`` with exactly one non-None.

    ``ctx`` is a store-building context manager the proxy owns; ``store`` is a
    caller-owned store the proxy must not close.
    """
    if store is not None:
        if backend is not None or url is not None:
            raise ValueError("PersistenceProxy.create: pass either store= or backend=/url=, not both")
        return None, store
    if backend is not None:
        if not url:
            raise ValueError("PersistenceProxy.create: backend= requires url=")
        name = backend.strip().lower()
        if name == "sqlite":
            return SQLiteEventStore.create(url, ttl=ttl), None
        if name == "redis":
            return RedisEventStore.create(url, ttl=ttl), None
        if name == "postgres":
            return PostgresEventStore.create(url, ttl=ttl), None
        raise ValueError(f"PersistenceProxy.create: unknown backend {name!r} (use sqlite, redis, or postgres)")
    if url is not None or ttl is not None:
        raise ValueError(
            "PersistenceProxy.create: url=/ttl= require backend=; with neither store= nor backend= set, "
            "the store is configured from MCP_PERSIST_* environment variables"
        )
    return event_store_from_env(), None
