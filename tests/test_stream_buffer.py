# pyright: reportPrivateUsage=false
"""Tests for StreamBuffer.

Two styles:

* Logic tests populate the buffer deterministically via ``_ingest`` (store +
  deque, exactly what the real consumer does) and then drive ``consume_from``.
  This pins down the cold/hot read paths without depending on task scheduling.
* Lifecycle tests run the real ``start``/``_consume`` path against a queue-driven
  fake response, exercising real concurrency, parser integration (with ``\\r\\n``
  framing), exception capture, and ``aclose``.

Async backend is asyncio (pytest-anyio default), matching the buffer's asyncio
primitives.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import aiosqlite
import pytest
from mcp.types import JSONRPCRequest

from mcp_persist import SQLiteEventStore
from mcp_persist._sse_parser import SSEFrame
from mcp_persist._stream_buffer import StreamBuffer

TABLE = "test_buffer_events"

PRIMING = SSEFrame(data="", event=None, original_id=None)


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


# ── helpers ────────────────────────────────────────────────────────────────


def payload(n: int) -> str:
    """A canonical JSON-RPC payload, serialized the way the store would.

    Because the bytes are already canonical, the hot path (raw frame data) and
    the cold path (re-serialized from the store) yield identical strings, so one
    ``expected`` value works for both.
    """
    return JSONRPCRequest(jsonrpc="2.0", id=n, method="ping").model_dump_json(by_alias=True, exclude_none=True)


def sse_chunk(data: str) -> str:
    """A live ``message`` event as the upstream (storeless, \\r\\n) would emit it."""
    return f"event: message\r\ndata: {data}\r\n\r\n"


async def ingest(buf: StreamBuffer, n: int) -> str:
    """Store + buffer event ``n``; return its proxy-assigned event id."""
    await buf._ingest(SSEFrame(data=payload(n), event="message", original_id=None))
    return buf._deque[-1][0]


async def collect(agen: AsyncIterator[tuple[str, str]]) -> list[str]:
    return [data async for _event_id, data in agen]


class FakeResponse:
    """Queue-driven stand-in for httpx.Response: the test scripts the stream."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue()
        self.closed = False

    async def push(self, chunk: str) -> None:
        await self._queue.put(("chunk", chunk))

    async def fail(self, exc: Exception) -> None:
        await self._queue.put(("raise", exc))

    async def end(self) -> None:
        await self._queue.put(("end", None))

    async def aiter_text(self) -> AsyncIterator[str]:
        while True:
            kind, value = await self._queue.get()
            if kind == "chunk":
                assert isinstance(value, str)
                yield value
            elif kind == "raise":
                assert isinstance(value, Exception)
                raise value
            else:
                return

    async def aclose(self) -> None:
        self.closed = True


# ── consume_from: cold/hot read paths ────────────────────────────────────────


@pytest.mark.anyio
async def test_fresh_stream_yields_in_order(store):
    buf = StreamBuffer("s:1", store)
    for n in (1, 2, 3):
        await ingest(buf, n)
    buf.done = True  # stream ended; a fresh consumer drains the window and returns

    got = await asyncio.wait_for(collect(buf.consume_from(None)), 5)
    assert got == [payload(1), payload(2), payload(3)]


@pytest.mark.anyio
async def test_reconnect_within_window_returns_delta(store):
    buf = StreamBuffer("s:1", store)
    e1 = await ingest(buf, 1)
    await ingest(buf, 2)
    await ingest(buf, 3)
    buf.done = True

    got = await asyncio.wait_for(collect(buf.consume_from(e1)), 5)
    assert got == [payload(2), payload(3)]  # everything strictly after e1, no replay needed


@pytest.mark.anyio
async def test_reconnect_past_window_replays_from_store(store):
    # maxlen=2 forces eviction; the early events survive only in the store.
    buf = StreamBuffer("s:1", store, maxlen=2)
    e1 = await ingest(buf, 1)
    await ingest(buf, 2)
    await ingest(buf, 3)
    await ingest(buf, 4)
    assert len(buf._deque) == 2  # only events 3,4 are live

    buf.done = True
    got = await asyncio.wait_for(collect(buf.consume_from(e1)), 5)
    assert got == [payload(2), payload(3), payload(4)]  # 2 from store (cold), 3,4 from window


@pytest.mark.anyio
async def test_cold_replay_transitions_to_live(store):
    # Reconnect from an evicted id: cold replay catches up from the store, then a
    # later live event is delivered hot from the deque.
    buf = StreamBuffer("s:1", store, maxlen=2)
    e1 = await ingest(buf, 1)
    await ingest(buf, 2)
    await ingest(buf, 3)  # deque holds 2,3; event 1 evicted

    agen = buf.consume_from(e1)
    cold = [await asyncio.wait_for(agen.__anext__(), 5) for _ in range(2)]
    assert [data for _eid, data in cold] == [payload(2), payload(3)]  # served from store

    await ingest(buf, 4)  # live event after the cold catch-up
    _eid, data = await asyncio.wait_for(agen.__anext__(), 5)
    assert data == payload(4)  # served hot from the window
    await agen.aclose()


@pytest.mark.anyio
async def test_consumer_attaching_after_done_gets_full_replay(store):
    buf = StreamBuffer("s:1", store)
    for n in (1, 2, 3):
        await ingest(buf, n)
    buf.done = True

    got = await asyncio.wait_for(collect(buf.consume_from(None)), 5)
    assert got == [payload(1), payload(2), payload(3)]


@pytest.mark.anyio
async def test_priming_event_visible_live(store):
    buf = StreamBuffer("s:1", store, maxlen=10)
    await buf._ingest(PRIMING)
    await ingest(buf, 1)
    buf.done = True

    got = await asyncio.wait_for(collect(buf.consume_from(None)), 5)
    assert got == ["", payload(1)]  # priming delivered on the live path


@pytest.mark.anyio
async def test_priming_event_excluded_from_cold_replay(store):
    buf = StreamBuffer("s:2", store, maxlen=1)
    e1 = await ingest(buf, 1)
    await buf._ingest(PRIMING)  # priming sits between e1 and the next real event
    await ingest(buf, 3)  # maxlen=1 -> deque holds only this; e1+priming are store-only
    buf.done = True

    got = await asyncio.wait_for(collect(buf.consume_from(e1)), 5)
    assert got == [payload(3)]  # store replay omits the priming event


@pytest.mark.anyio
async def test_cold_replay_blocks_foreign_stream(store):
    # A cursor pointing at *another* stream's event must not replay that stream:
    # replay_events_after resolves the stream from the global, enumerable event id,
    # so without the ownership guard a consumer of buffer B could read buffer A's
    # history by guessing A's event id.
    victim = StreamBuffer("s:victim", store)
    foreign_id = await ingest(victim, 1)
    await ingest(victim, 2)

    attacker = StreamBuffer("s:attacker", store)
    attacker.done = True  # empty window; a cold cursor would otherwise hit the store
    got = await asyncio.wait_for(collect(attacker.consume_from(foreign_id)), 5)
    assert got == []  # foreign replay blocked; nothing leaked


@pytest.mark.anyio
async def test_cold_replay_allows_own_stream(store):
    # The ownership guard must not regress same-stream gap replay: an evicted
    # cursor on this buffer's own stream still replays from the store.
    buf = StreamBuffer("s:1", store, maxlen=1)
    e1 = await ingest(buf, 1)
    await ingest(buf, 2)  # maxlen=1 -> event 1 lives only in the store
    buf.done = True

    got = await asyncio.wait_for(collect(buf.consume_from(e1)), 5)
    assert got == [payload(2)]  # own-stream replay still works


@pytest.mark.anyio
async def test_non_numeric_last_event_id_resumes_from_live_tail(store):
    buf = StreamBuffer("s:1", store)
    await ingest(buf, 1)
    last = await ingest(buf, 2)
    buf.done = True

    got = await asyncio.wait_for(collect(buf.consume_from("not-a-number")), 5)
    assert got == []  # resumed from the tail; nothing newer, no crash, no window dump
    assert buf._normalize_cursor("not-a-number") == last


# ── start / _consume lifecycle ───────────────────────────────────────────────


@pytest.mark.anyio
async def test_start_consumes_live_stream_and_closes_response(store):
    buf = StreamBuffer("s:1", store)
    fake = FakeResponse()
    buf.start(fake)

    async def produce():
        for n in (1, 2):
            await fake.push(sse_chunk(payload(n)))
        await fake.end()

    got, _ = await asyncio.wait_for(asyncio.gather(collect(buf.consume_from(None)), produce()), 5)
    assert got == [payload(1), payload(2)]
    assert buf._task is not None
    await asyncio.wait_for(buf._task, 5)
    assert buf.done
    assert fake.closed


@pytest.mark.anyio
async def test_concurrent_consumers_each_get_all_events(store):
    buf = StreamBuffer("s:1", store)
    fake = FakeResponse()
    buf.start(fake)

    async def produce():
        for n in (1, 2, 3):
            await fake.push(sse_chunk(payload(n)))
        await fake.end()

    r1, r2, _ = await asyncio.wait_for(
        asyncio.gather(
            collect(buf.consume_from(None)),
            collect(buf.consume_from(None)),
            produce(),
        ),
        5,
    )
    assert r1 == [payload(1), payload(2), payload(3)]
    assert r2 == [payload(1), payload(2), payload(3)]


@pytest.mark.anyio
async def test_consume_exception_is_captured_and_reraised(store):
    buf = StreamBuffer("s:1", store)
    fake = FakeResponse()
    buf.start(fake)
    await fake.push(sse_chunk(payload(1)))
    await fake.fail(RuntimeError("boom"))

    seen: list[str] = []
    with pytest.raises(RuntimeError, match="boom"):
        async for _eid, data in buf.consume_from(None):
            seen.append(data)
    assert seen == [payload(1)]  # buffered events are delivered before the error surfaces

    assert buf._task is not None
    await asyncio.wait_for(buf._task, 5)
    assert isinstance(buf._exc, RuntimeError)


@pytest.mark.anyio
async def test_response_closed_even_when_consume_raises(store):
    buf = StreamBuffer("s:1", store)
    fake = FakeResponse()
    buf.start(fake)
    await fake.fail(RuntimeError("boom"))

    assert buf._task is not None
    await asyncio.wait_for(buf._task, 5)  # _consume captures the error, so the task completes
    assert fake.closed
    assert isinstance(buf._exc, RuntimeError)


@pytest.mark.anyio
async def test_aclose_cancels_task_and_closes_response(store):
    buf = StreamBuffer("s:1", store)
    fake = FakeResponse()
    buf.start(fake)  # never receives an end; only aclose stops it
    await asyncio.sleep(0)  # let _consume reach its first await before cancelling

    await buf.aclose()
    assert buf._task is not None and buf._task.done()
    assert fake.closed  # _consume's finally closed the upstream response
    assert buf.done
    await buf.aclose()  # idempotent


@pytest.mark.anyio
async def test_aclose_before_start_is_noop(store):
    await StreamBuffer("s:1", store).aclose()  # no task yet -> nothing to do


@pytest.mark.anyio
async def test_start_twice_raises(store):
    buf = StreamBuffer("s:1", store)
    fake = FakeResponse()
    buf.start(fake)
    with pytest.raises(RuntimeError, match="already started"):
        buf.start(FakeResponse())

    # The first stream never ends; cancel its task so it doesn't dangle.
    assert buf._task is not None
    buf._task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await buf._task


# ── proxy replay metrics (on_proxy_replay) ───────────────────────────────────


class ProxyReplayRecorder:
    """Captures on_proxy_replay calls; deliberately has only that one hook."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def on_proxy_replay(self, stream_id, session_id, events_replayed, blocked, duration_ms) -> None:
        self.calls.append((stream_id, session_id, events_replayed, blocked, duration_ms))


@pytest.mark.anyio
async def test_proxy_replay_metric_fires_on_cold_replay(store):
    # maxlen=2 evicts events 1,2 from the live window but they remain in the store,
    # so a consumer resuming after event 1 must replay 2,3,4 cold.
    rec = ProxyReplayRecorder()
    buf = StreamBuffer("sess-1:s", store, maxlen=2, metrics=rec)
    e1 = await ingest(buf, 1)
    await ingest(buf, 2)
    await ingest(buf, 3)
    await ingest(buf, 4)
    buf.done = True

    out = await collect(buf.consume_from(after=e1))

    assert out == [payload(2), payload(3), payload(4)]
    assert len(rec.calls) == 1
    stream_id, session_id, events, blocked, duration_ms = rec.calls[0]
    assert (stream_id, session_id, events, blocked) == ("sess-1:s", "sess-1", 3, False)
    assert duration_ms >= 0


@pytest.mark.anyio
async def test_proxy_replay_metric_marks_blocked_cross_stream(store):
    # Store holds another stream's events; a buffer for a different stream that
    # resumes from a foreign event id must replay nothing and record blocked=True.
    foreign = StreamBuffer("sess-1:s", store, maxlen=1024)
    e1 = await ingest(foreign, 1)
    await ingest(foreign, 2)

    rec = ProxyReplayRecorder()
    buf = StreamBuffer("sess-2:other", store, maxlen=1024, metrics=rec)
    buf.done = True

    out = await collect(buf.consume_from(after=e1))

    assert out == []
    assert len(rec.calls) == 1
    stream_id, session_id, events, blocked, _duration_ms = rec.calls[0]
    assert (stream_id, session_id, events, blocked) == ("sess-2:other", "sess-2", 0, True)


@pytest.mark.anyio
async def test_no_metrics_collector_is_inert(store):
    # The default (no metrics=) must not raise on the cold path.
    buf = StreamBuffer("sess-1:s", store, maxlen=2)
    e1 = await ingest(buf, 1)
    await ingest(buf, 2)
    await ingest(buf, 3)
    buf.done = True
    out = await collect(buf.consume_from(after=e1))
    assert out == [payload(2), payload(3)]
