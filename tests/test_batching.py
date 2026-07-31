# pyright: reportPrivateUsage=false
# pyright: reportArgumentType=false
# pyright: reportMissingImports=false
"""Tests for BatchingEventStore.

Runs against fakeredis (no server) wrapping a real RedisEventStore, plus a
rejection test for the unsupported SQLite backend.
"""

from __future__ import annotations

import asyncio

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.server.streamable_http import EventMessage
from mcp_types import JSONRPCRequest

from mcp_persist import BatchingEventStore, RedisEventStore, SQLiteEventStore

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


@pytest.fixture
async def redis_store():
    client = fakeredis.FakeRedis()
    await client.flushdb()
    store = RedisEventStore(client, ttl=3600)
    try:
        yield store
    finally:
        await client.flushdb()
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


async def _replay(store, last_event_id):
    captured: list[EventMessage] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    await store.replay_events_after(last_event_id, cb)
    return captured


@pytest.mark.anyio
async def test_ids_are_monotonic_and_immediate(redis_store):
    batching = BatchingEventStore(redis_store, flush_max_events=8, flush_max_latency_ms=20)
    ids = [await batching.store_event("s", SAMPLE_MSG) for _ in range(5)]
    # Returned synchronously and strictly increasing, before any flush window.
    assert ids == sorted(ids, key=int)
    assert len(set(ids)) == 5
    await batching.aclose()


@pytest.mark.anyio
async def test_flush_on_size_threshold(redis_store):
    batching = BatchingEventStore(redis_store, flush_max_events=4, flush_max_latency_ms=10_000)
    first = await batching.store_event("s", SAMPLE_MSG)
    for _ in range(3):  # reaches flush_max_events=4 -> synchronous flush
        await batching.store_event("s", SAMPLE_MSG)
    # All four are durable in Redis even though the latency window has not elapsed.
    captured = await _replay(redis_store, first)
    assert len(captured) == 3  # events after the anchor
    await batching.aclose()


@pytest.mark.anyio
async def test_flush_on_latency_ceiling(redis_store):
    batching = BatchingEventStore(redis_store, flush_max_events=1000, flush_max_latency_ms=30)
    first = await batching.store_event("s", SAMPLE_MSG)
    await batching.store_event("s", SAMPLE_MSG)
    # Nothing flushed yet (size threshold not hit); wait past the latency window.
    await asyncio.sleep(0.1)
    captured = await _replay(redis_store, first)
    assert len(captured) == 1
    await batching.aclose()


@pytest.mark.anyio
async def test_replay_flushes_pending(redis_store):
    batching = BatchingEventStore(redis_store, flush_max_events=1000, flush_max_latency_ms=10_000)
    first = await batching.store_event("s", SAMPLE_MSG)
    await batching.store_event("s", SAMPLE_MSG)
    # replay() must flush buffered writes so a reconnecting client sees them.
    captured = await _replay(batching, first)
    assert len(captured) == 1
    await batching.aclose()


@pytest.mark.anyio
async def test_aclose_flushes_remaining(redis_store):
    batching = BatchingEventStore(redis_store, flush_max_events=1000, flush_max_latency_ms=10_000)
    first = await batching.store_event("s", SAMPLE_MSG)
    await batching.store_event("s", SAMPLE_MSG)
    await batching.aclose()
    captured = await _replay(redis_store, first)
    assert len(captured) == 1


@pytest.mark.anyio
async def test_failed_flush_retains_unwritten_tail_and_recovers(redis_store, monkeypatch):
    batching = BatchingEventStore(redis_store, flush_max_events=3, flush_max_latency_ms=10_000)
    real_store = redis_store._store_event_with_id
    monkeypatch.setattr(redis_store, "_store_events_with_ids", None)
    calls = 0

    async def fail_second_write(stream_id, message, event_id):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ConnectionError("transient write failure")
        await real_store(stream_id, message, event_id)

    monkeypatch.setattr(redis_store, "_store_event_with_id", fail_second_write)
    first = await batching.store_event("s", SAMPLE_MSG)
    await batching.store_event("s", SAMPLE_MSG)
    with pytest.raises(ConnectionError, match="transient"):
        await batching.store_event("s", SAMPLE_MSG)

    # The completed head is not retried, while the failed event and untouched
    # tail remain queued in their original order.
    assert [item.event_id for item in batching._pending] == ["2", "3"]
    await batching.flush()
    captured = await _replay(redis_store, first)
    assert [event.event_id for event in captured] == ["2", "3"]
    await batching.aclose()


@pytest.mark.anyio
async def test_flush_uses_backend_bulk_write(redis_store, monkeypatch):
    real_store_batch = redis_store._store_events_with_ids
    batches = []

    async def recording_batch(events):
        batches.append(events)
        await real_store_batch(events)

    monkeypatch.setattr(redis_store, "_store_events_with_ids", recording_batch)
    batching = BatchingEventStore(redis_store, flush_max_events=3, flush_max_latency_ms=10_000)
    ids = [await batching.store_event("s", SAMPLE_MSG) for _ in range(3)]

    assert len(batches) == 1
    assert [event_id for _stream_id, _message, event_id in batches[0]] == ids
    await batching.aclose()


@pytest.mark.anyio
async def test_store_event_rejected_after_close(redis_store):
    batching = BatchingEventStore(redis_store)
    await batching.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await batching.store_event("s", SAMPLE_MSG)


@pytest.mark.anyio
async def test_operational_apis_delegate_and_flush_pending_writes(redis_store):
    batching = BatchingEventStore(redis_store, flush_max_events=1000, flush_max_latency_ms=10_000)
    event_id = await batching.store_event("s", SAMPLE_MSG)

    # Health is useful before a forced flush and exposes the batching state.
    report = await batching.health()
    assert report.healthy is True
    assert report.backend == "batching"
    assert report.detail["inner_backend"] == "redis"
    assert report.detail["pending_writes"] == 1

    # All operational reads flush first, so they observe the event immediately.
    assert await batching._event_exists(event_id)
    assert [stream async for stream in batching.list_streams()] == ["s"]
    events = [item async for item in batching._iter_stream_events("s")]
    assert [stored_id for stored_id, _message in events] == [event_id]
    assert await batching.ping() is True
    await batching.aclose()


@pytest.mark.anyio
async def test_validation_rejects_bad_args(redis_store):
    with pytest.raises(ValueError, match="flush_max_events"):
        BatchingEventStore(redis_store, flush_max_events=0)
    with pytest.raises(ValueError, match="flush_max_latency_ms"):
        BatchingEventStore(redis_store, flush_max_latency_ms=0)


@pytest.mark.anyio
async def test_sqlite_is_rejected():
    conn = await aiosqlite.connect(":memory:")
    try:
        store = SQLiteEventStore(conn, table_name="t", ttl=3600)
        await store.initialize()
        with pytest.raises(TypeError, match="write-behind"):
            BatchingEventStore(store)
    finally:
        await conn.close()
