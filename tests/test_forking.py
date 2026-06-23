# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Tests for event stream forking."""

from __future__ import annotations

import asyncio
import os

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import PostgresEventStore, RedisEventStore, SQLiteEventStore


def SAMPLE_MSG(step: int) -> JSONRPCRequest:
    return JSONRPCRequest(jsonrpc="2.0", id=str(step), method=f"step_{step}")


@pytest.fixture
async def sqlite_conn():
    connection = await aiosqlite.connect(":memory:")
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
async def sqlite_store(sqlite_conn):
    s = SQLiteEventStore(sqlite_conn, table_name="test_events", ttl=None)
    await s.initialize()
    return s


@pytest.fixture
async def redis_client():
    client = fakeredis.FakeRedis()
    try:
        yield client
    finally:
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


@pytest.fixture
def redis_store(redis_client):
    return RedisEventStore(redis_client, key_prefix="test:", ttl=None)


async def run_forking_scenario(store):
    # Step 1: Write first 4 events to orig-stream
    await store.store_event("orig-stream", SAMPLE_MSG(1))
    eid2 = await store.store_event("orig-stream", SAMPLE_MSG(2))
    await store.store_event("orig-stream", SAMPLE_MSG(3))
    eid4 = await store.store_event("orig-stream", SAMPLE_MSG(4))

    # Fork at step 4
    await store.fork_stream("orig-stream", eid4, "fork-model-a")
    await store.fork_stream("orig-stream", eid4, "fork-model-b")

    # Step 2: Write different steps to each branch
    await store.store_event("fork-model-a", SAMPLE_MSG(51))
    await store.store_event("fork-model-a", SAMPLE_MSG(52))

    await store.store_event("fork-model-b", SAMPLE_MSG(61))
    await store.store_event("fork-model-b", SAMPLE_MSG(62))

    # Step 3: Replay from fork-model-a and verify history (should contain 1, 2, 3, 4, 51, 52)
    events_a = []

    async def cb_a(event: EventMessage) -> None:
        events_a.append(event)

    await store.replay_events_after("0", cb_a, "fork-model-a")

    ids_a = [e.message.root.id for e in events_a]
    assert ids_a == ["1", "2", "3", "4", "51", "52"]

    # Step 4: Replay from fork-model-b and verify history (should contain 1, 2, 3, 4, 61, 62)
    events_b = []

    async def cb_b(event: EventMessage) -> None:
        events_b.append(event)

    await store.replay_events_after("0", cb_b, "fork-model-b")

    ids_b = [e.message.root.id for e in events_b]
    assert ids_b == ["1", "2", "3", "4", "61", "62"]

    # Step 5: Replay from fork-model-a starting *after* step 2 (should yield 3, 4, 51, 52)
    events_after_2 = []

    async def cb_after_2(event: EventMessage) -> None:
        events_after_2.append(event)

    await store.replay_events_after(eid2, cb_after_2, "fork-model-a")

    ids_after_2 = [e.message.root.id for e in events_after_2]
    assert ids_after_2 == ["3", "4", "51", "52"]

    # Step 6: Test migration iteration (_iter_stream_events) on fork-model-a
    iter_events = []
    async for _, message in store._iter_stream_events("fork-model-a"):
        if message is not None:
            iter_events.append(message.root.id)
    assert iter_events == ["1", "2", "3", "4", "51", "52"]


@pytest.mark.anyio
async def test_forking_sqlite(sqlite_store):
    await run_forking_scenario(sqlite_store)


@pytest.mark.anyio
async def test_forking_redis(redis_store):
    await run_forking_scenario(redis_store)


# Conditional Postgres test
POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL")


@pytest.mark.skipif(POSTGRES_URL is None, reason="No Postgres URL configured")
@pytest.mark.anyio
async def test_forking_postgres():
    import asyncpg

    pool = await asyncpg.create_pool(POSTGRES_URL)
    try:
        await pool.execute("DROP TABLE IF EXISTS test_events")
        await pool.execute("DROP TABLE IF EXISTS test_events_forks")
        s = PostgresEventStore(pool, table_name="test_events", ttl=None)
        await s.initialize()
        await run_forking_scenario(s)
    finally:
        await pool.execute("DROP TABLE IF EXISTS test_events")
        await pool.execute("DROP TABLE IF EXISTS test_events_forks")
        await pool.close()


# ── Stress Tests ─────────────────────────────────────────────────────────────


async def run_deep_forking_stress(store):
    # Fork 50 levels deep.
    # Level 0 has 1 event.
    # Each level i has 1 event and forks to level i+1 at that event.
    current_stream = "orig-stream"
    eid = await store.store_event(current_stream, SAMPLE_MSG(0))
    expected_ids = ["0"]

    for i in range(1, 51):
        next_stream = f"fork-level-{i}"
        await store.fork_stream(current_stream, eid, next_stream)
        eid = await store.store_event(next_stream, SAMPLE_MSG(i))
        expected_ids.append(str(i))
        current_stream = next_stream

    # Replay deepest fork
    events = []

    async def cb(event: EventMessage) -> None:
        events.append(event)

    await store.replay_events_after("0", cb, current_stream)
    assert [e.message.root.id for e in events] == expected_ids


async def run_wide_forking_stress(store):
    # Fork 300 child streams from a single event.
    parent = "orig-stream"
    eid = await store.store_event(parent, SAMPLE_MSG(0))

    child_streams = [f"child-{i}" for i in range(300)]
    for child in child_streams:
        await store.fork_stream(parent, eid, child)

    # Write one event to each child stream
    for i, child in enumerate(child_streams):
        await store.store_event(child, SAMPLE_MSG(i + 1))

    # Replay and verify a subset of child streams concurrently
    async def verify_child(child_idx):
        events = []

        async def cb(event: EventMessage) -> None:
            events.append(event)

        child_stream = child_streams[child_idx]
        await store.replay_events_after("0", cb, child_stream)
        assert [e.message.root.id for e in events] == ["0", str(child_idx + 1)]

    tasks = [verify_child(i) for i in range(100)]
    await asyncio.gather(*tasks)


@pytest.mark.anyio
async def test_deep_forking_sqlite(sqlite_store):
    await run_deep_forking_stress(sqlite_store)


@pytest.mark.anyio
async def test_deep_forking_redis(redis_store):
    await run_deep_forking_stress(redis_store)


@pytest.mark.anyio
async def test_wide_forking_sqlite(sqlite_store):
    await run_wide_forking_stress(sqlite_store)


@pytest.mark.anyio
async def test_wide_forking_redis(redis_store):
    await run_wide_forking_stress(redis_store)
