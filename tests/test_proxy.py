# pyright: reportPrivateUsage=false
"""Tests for PersistenceProxy.

httpx.ASGITransport buffers a whole response before returning and only signals
``http.disconnect`` after completion, so it can't drive a live SSE stream or a
mid-stream disconnect. Instead:

* the upstream is a custom httpx transport whose response body is a lazy,
  queue-driven stream the test feeds event by event, and
* the client drives the proxy ASGI app directly via controllable
  ``receive``/``send``, so frames can be read live and a disconnect injected at
  will.

Everything runs in-memory on the asyncio backend.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import aiosqlite
import httpx
import pytest
from mcp.types import JSONRPCNotification

from mcp_persist import SQLiteEventStore
from mcp_persist._sse_parser import SSEFrame, SSEParser
from mcp_persist.proxy import PersistenceProxy

TABLE = "test_proxy_events"


@pytest.fixture
async def conn():
    connection = await aiosqlite.connect(":memory:")
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
async def store(conn):
    s = SQLiteEventStore(conn, table_name=TABLE, ttl=None)
    await s.initialize()
    return s


@pytest.fixture
async def make(store):
    """Return a builder for proxies sharing the test's store; close them on teardown."""
    proxies: list[PersistenceProxy] = []

    def _make(upstream: ControlledUpstream) -> PersistenceProxy:
        # follow_redirects mirrors the production client; without it an upstream
        # trailing-slash redirect would be handed to the client, which would
        # follow it past the proxy.
        client = httpx.AsyncClient(transport=upstream, base_url="http://up", follow_redirects=True)
        proxy = PersistenceProxy("http://up", store, client=client, buffer_grace_ttl=60.0)
        proxies.append(proxy)
        return proxy

    yield _make
    for proxy in proxies:
        await proxy.close_all()


# ── controllable upstream (custom httpx transport) ───────────────────────────


def payload(tag: str) -> str:
    """A canonical JSON-RPC notification, serialized the way the store would.

    Canonical bytes mean the hot path (raw frame) and the cold path
    (re-serialized from the store) produce identical strings, so one expected
    value works for both. ``tag`` (the method name) makes events distinguishable.
    """
    return JSONRPCNotification(jsonrpc="2.0", method=tag).model_dump_json(by_alias=True, exclude_none=True)


def sse(tag: str) -> bytes:
    """A storeless upstream's SSE frame: event + JSON-RPC data, no id (proxy assigns ids)."""
    return f"event: message\r\ndata: {payload(tag)}\r\n\r\n".encode()


class _QueueStream(httpx.AsyncByteStream):
    """A lazy response body: yields chunks fed onto a queue; ``None`` ends it."""

    def __init__(self, queue: asyncio.Queue[bytes | None]) -> None:
        self._queue = queue

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = await self._queue.get()
            if chunk is None:
                return
            yield chunk

    async def aclose(self) -> None:
        pass


class ControlledUpstream(httpx.AsyncBaseTransport):
    """Fake upstream MCP server with test-controlled streaming responses.

    A custom transport rather than httpx.ASGITransport because ASGITransport
    runs the app to completion and buffers the whole body before returning the
    response (see its source) — so it cannot model an upstream SSE stream that
    stays open while the proxy reads it. Here the response body is a lazy
    ``_QueueStream`` the test feeds incrementally.
    """

    def __init__(self) -> None:
        self.session_id = "sess-abc"
        self.post_json: bytes | None = None  # if set, POST returns JSON
        self.redirect_post = False  # if set, POST /mcp -> 307 /mcp/ (real servers do this)
        self.post_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.get_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.post_count = 0
        self.get_count = 0
        self.requests: list[tuple[str, dict[str, str]]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in request.headers.raw}
        await request.aread()
        self.requests.append((request.method, headers))
        if request.url.path not in ("/mcp", "/mcp/"):  # non-MCP paths: plain passthrough target
            return httpx.Response(200, headers=[(b"content-type", b"application/json")], content=b'{"ok":true}')
        if self.redirect_post and request.method == "POST" and request.url.path == "/mcp":
            return httpx.Response(307, headers=[(b"location", b"http://up/mcp/")])  # trailing-slash redirect
        sse_headers = [
            (b"content-type", b"text/event-stream; charset=utf-8"),
            (b"mcp-session-id", self.session_id.encode()),
        ]
        if request.method == "POST":
            self.post_count += 1
            if self.post_json is not None:
                json_headers = [(b"content-type", b"application/json"), (b"mcp-session-id", self.session_id.encode())]
                return httpx.Response(200, headers=json_headers, content=self.post_json)
            return httpx.Response(200, headers=sse_headers, stream=_QueueStream(self.post_queue))
        if request.method == "GET":
            self.get_count += 1
            return httpx.Response(200, headers=sse_headers, stream=_QueueStream(self.get_queue))
        return httpx.Response(200, headers=[(b"content-type", b"application/json")], content=b'{"ok":true}')

    def feed(self, queue: asyncio.Queue[bytes | None], *datas: str, end: bool = True) -> None:
        for data in datas:
            queue.put_nowait(sse(data))
        if end:
            queue.put_nowait(None)


# ── client that drives the proxy ASGI app directly ───────────────────────────


class Client:
    def __init__(
        self,
        proxy: PersistenceProxy,
        method: str,
        *,
        path: str = "/mcp",
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        query: bytes = b"",
    ) -> None:
        self.scope = {
            "type": "http",
            "method": method,
            "path": path,
            "query_string": query,
            "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in (headers or {}).items()],
        }
        self._incoming: asyncio.Queue[dict] = asyncio.Queue()
        self._incoming.put_nowait({"type": "http.request", "body": body, "more_body": False})
        self._messages: asyncio.Queue[dict] = asyncio.Queue()
        self._parser = SSEParser()
        self.task = asyncio.create_task(proxy(self.scope, self._receive, self._send))

    async def _receive(self) -> dict:
        return await self._incoming.get()

    async def _send(self, message: dict) -> None:
        self._messages.put_nowait(message)

    def disconnect(self) -> None:
        self._incoming.put_nowait({"type": "http.disconnect"})

    async def start(self) -> tuple[int, dict[str, str]]:
        message = await asyncio.wait_for(self._messages.get(), 5)
        assert message["type"] == "http.response.start", message
        return message["status"], {k.decode("latin-1"): v.decode("latin-1") for k, v in message["headers"]}

    async def _body(self) -> tuple[bytes, bool]:
        message = await asyncio.wait_for(self._messages.get(), 5)
        assert message["type"] == "http.response.body", message
        return message["body"], message.get("more_body", False)

    async def read_json(self) -> bytes:
        body, more = await self._body()
        assert not more
        return body

    async def read_events(self, count: int) -> list[SSEFrame]:
        out: list[SSEFrame] = []
        while len(out) < count:
            body, more = await self._body()
            out.extend(self._parser.feed(body.decode()))
            if not more:
                out.extend(self._parser.flush())
                break
        return out

    async def read_all(self) -> list[SSEFrame]:
        out: list[SSEFrame] = []
        while True:
            body, more = await self._body()
            out.extend(self._parser.feed(body.decode()))
            if not more:
                out.extend(self._parser.flush())
                return out

    async def finish(self) -> None:
        await asyncio.wait_for(self.task, 5)


# ── tests ────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_post_json_passthrough(make):
    up = ControlledUpstream()
    up.post_json = b'{"jsonrpc":"2.0","id":1,"result":{}}'
    proxy = make(up)

    c = Client(
        proxy,
        "POST",
        headers={"content-type": "application/json", "mcp-session-id": "CLIENT"},
        body=b'{"jsonrpc":"2.0","id":1,"method":"x"}',
    )
    status, headers = await c.start()
    assert status == 200
    assert json.loads(await c.read_json()) == {"jsonrpc": "2.0", "id": 1, "result": {}}
    assert headers["mcp-session-id"] == up.session_id  # session forwarded to client
    await c.finish()

    assert up.requests[0][1]["mcp-session-id"] == "CLIENT"  # and to upstream
    assert proxy._buffers == {}  # nothing buffered for a JSON response


@pytest.mark.anyio
async def test_post_sse_complete(make):
    up = ControlledUpstream()
    up.feed(up.post_queue, "a", "b", "c")
    proxy = make(up)

    c = Client(proxy, "POST", body=b'{"jsonrpc":"2.0","id":7,"method":"x"}')
    status, headers = await c.start()
    assert status == 200
    assert headers["content-type"].startswith("text/event-stream")
    assert headers["mcp-session-id"] == up.session_id

    events = await c.read_all()
    assert [e.data for e in events] == [payload("a"), payload("b"), payload("c")]
    assert [e.original_id for e in events] == ["1", "2", "3"]  # proxy-assigned, monotonic
    await c.finish()
    assert f"{up.session_id}:7" in proxy._buffers  # keyed by session:request_id


@pytest.mark.anyio
async def test_post_sse_follows_upstream_trailing_slash_redirect(make):
    # Real MCP servers mount at /mcp and 307 a bare /mcp to /mcp/. The proxy must
    # follow that redirect itself — otherwise the client follows it past the proxy
    # (straight to the upstream) and bypasses resumability entirely.
    up = ControlledUpstream()
    up.redirect_post = True
    up.feed(up.post_queue, "a", "b")
    proxy = make(up)

    c = Client(proxy, "POST", body=b'{"jsonrpc":"2.0","id":7,"method":"x"}')
    status, headers = await c.start()
    assert status == 200  # the 307 was resolved inside the proxy
    assert headers["content-type"].startswith("text/event-stream")
    events = await c.read_all()
    assert [e.data for e in events] == [payload("a"), payload("b")]
    await c.finish()
    assert [m for m, _ in up.requests] == ["POST", "POST"]  # /mcp then /mcp/


@pytest.mark.anyio
async def test_get_fresh(make):
    up = ControlledUpstream()
    up.feed(up.get_queue, "n1", "n2", "n3")
    proxy = make(up)

    c = Client(proxy, "GET", headers={"mcp-session-id": "S1"})
    status, _ = await c.start()
    assert status == 200
    events = await c.read_all()
    assert [e.data for e in events] == [payload("n1"), payload("n2"), payload("n3")]
    await c.finish()

    assert up.get_count == 1
    assert up.requests[0][1]["mcp-session-id"] == "S1"  # forwarded
    assert "last-event-id" not in up.requests[0][1]  # stripped on the upstream GET


@pytest.mark.anyio
async def test_get_reconnect_live_buffer(make):
    up = ControlledUpstream()
    proxy = make(up)

    # Client A: fresh GET, receive one notification, then disconnect.
    a = Client(proxy, "GET", headers={"mcp-session-id": "S1"})
    await a.start()
    up.get_queue.put_nowait(sse("n1"))
    (event,) = await a.read_events(1)
    assert event.data == payload("n1")
    last_id = event.original_id
    assert last_id is not None  # the proxy assigns every forwarded event an id
    a.disconnect()
    await a.finish()  # handler returns on disconnect; the buffer lives on

    assert not proxy._buffers["S1:_GET_stream"].done

    # Client B: reconnect — should reuse the live buffer (no second upstream GET).
    b = Client(proxy, "GET", headers={"mcp-session-id": "S1", "last-event-id": last_id})
    await b.start()
    up.get_queue.put_nowait(sse("n2"))
    up.get_queue.put_nowait(None)
    events = await b.read_all()
    assert [e.data for e in events] == [payload("n2")]  # gap (none) + live continuation
    await b.finish()

    assert up.get_count == 1  # the live buffer was reused — 409 avoided


@pytest.mark.anyio
async def test_get_reconnect_store_only(make):
    up = ControlledUpstream()
    up.feed(up.post_queue, "a", "b", "c")
    proxy = make(up)

    # Store a,b,c via a POST SSE stream.
    p = Client(proxy, "POST", headers={"mcp-session-id": "S1"}, body=b'{"jsonrpc":"2.0","id":9,"method":"x"}')
    _, post_headers = await p.start()
    session = post_headers["mcp-session-id"]
    events = await p.read_all()
    await p.finish()
    first_id = events[0].original_id
    assert first_id is not None

    # Simulate the buffer having been evicted after its grace period.
    proxy._buffers.clear()

    # Reconnect with the upstream GET ending immediately: replay from store only.
    up.get_queue.put_nowait(None)
    r = Client(proxy, "GET", headers={"mcp-session-id": session, "last-event-id": first_id})
    await r.start()
    replayed = await r.read_all()
    assert [e.data for e in replayed] == [payload("b"), payload("c")]  # store replay after the first event
    await r.finish()
    assert up.get_count == 1  # one fresh upstream GET opened to resume notifications


@pytest.mark.anyio
async def test_post_disconnect_then_reconnect_replays_remainder(make):
    up = ControlledUpstream()
    proxy = make(up)

    p = Client(proxy, "POST", headers={"mcp-session-id": "S1"}, body=b'{"jsonrpc":"2.0","id":5,"method":"x"}')
    _, post_headers = await p.start()
    session = post_headers["mcp-session-id"]

    # Deliver 2 of 5 events, then the client disconnects mid-stream.
    up.post_queue.put_nowait(sse("e1"))
    up.post_queue.put_nowait(sse("e2"))
    seen = await p.read_events(2)
    assert [e.data for e in seen] == [payload("e1"), payload("e2")]
    second_id = seen[1].original_id
    assert second_id is not None
    p.disconnect()
    await p.finish()

    buf = proxy._buffers[f"{session}:5"]
    assert not buf.done  # the buffer survives the client disconnect

    # The upstream sends the rest; the buffer stores them with no client attached.
    up.feed(up.post_queue, "e3", "e4", "e5")
    assert buf._task is not None
    await asyncio.wait_for(buf._task, 5)
    assert buf.done

    # Reconnect via GET + Last-Event-ID: replay the events missed after e2.
    up.get_queue.put_nowait(None)
    r = Client(proxy, "GET", headers={"mcp-session-id": session, "last-event-id": second_id})
    await r.start()
    replayed = await r.read_all()
    assert [e.data for e in replayed] == [payload("e3"), payload("e4"), payload("e5")]
    await r.finish()


@pytest.mark.anyio
async def test_get_reconnect_cross_session_replay_blocked(make):
    # Event ids are global and sequential, so a client can guess another session's
    # id. The proxy must not replay a stream whose id doesn't belong to the
    # requesting session, or it leaks one session's history to another.
    up = ControlledUpstream()
    up.feed(up.post_queue, "a", "b", "c")
    proxy = make(up)

    # Victim stores a,b,c via a POST SSE stream.
    p = Client(proxy, "POST", headers={"mcp-session-id": "VICTIM"}, body=b'{"jsonrpc":"2.0","id":9,"method":"x"}')
    _, post_headers = await p.start()
    victim_session = post_headers["mcp-session-id"]
    events = await p.read_all()
    await p.finish()
    first_id = events[0].original_id
    assert first_id is not None

    # Drop the live buffers so the GET reconnect falls through to store replay.
    proxy._buffers.clear()

    # Attacker: a *different* session reconnects with the victim's event id. The
    # fresh upstream GET ends immediately, so anything received came from replay.
    up.get_queue.put_nowait(None)
    attacker = Client(proxy, "GET", headers={"mcp-session-id": "ATTACKER", "last-event-id": first_id})
    await attacker.start()
    leaked = await attacker.read_all()
    await attacker.finish()
    assert leaked == []  # cross-session replay blocked: none of VICTIM's events leak

    # Control: the legitimate owner (same session) still gets its replay, proving
    # the events were stored and the block is the ownership check, not an empty store.
    proxy._buffers.clear()
    up.get_queue.put_nowait(None)
    owner = Client(proxy, "GET", headers={"mcp-session-id": victim_session, "last-event-id": first_id})
    await owner.start()
    replayed = await owner.read_all()
    await owner.finish()
    assert [e.data for e in replayed] == [payload("b"), payload("c")]


@pytest.mark.anyio
async def test_post_body_too_large_returns_413(make):
    up = ControlledUpstream()
    proxy = make(up)
    proxy._max_request_body_bytes = 16  # tiny cap so a normal JSON-RPC body trips it

    c = Client(proxy, "POST", body=b'{"jsonrpc":"2.0","id":1,"method":"way-too-long"}')
    status, _ = await c.start()
    assert status == 413
    await c.read_json()  # drain the error body
    await c.finish()
    assert up.post_count == 0  # rejected before it was ever forwarded upstream


@pytest.mark.anyio
async def test_non_mcp_path_passthrough(make):
    up = ControlledUpstream()
    proxy = make(up)

    c = Client(proxy, "GET", path="/health")
    status, _ = await c.start()
    assert status == 200
    assert json.loads(await c.read_json()) == {"ok": True}
    await c.finish()
    assert up.requests[0][0] == "GET"
