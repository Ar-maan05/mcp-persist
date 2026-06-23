# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Production-grade stress tests for event stream forking."""

from __future__ import annotations

import asyncio
import os
import random
import time

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
    # Tune SQLite for higher concurrency and throughput in tests
    await connection.execute("PRAGMA journal_mode=WAL")
    await connection.execute("PRAGMA synchronous=NORMAL")
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
async def sqlite_store(sqlite_conn):
    s = SQLiteEventStore(sqlite_conn, table_name="stress_fork_events", ttl=None)
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
    return RedisEventStore(redis_client, key_prefix="stress_fork:", ttl=None)


async def run_production_stress_test(store, num_forks=30, writes_per_fork=50):
    # Step 1: Write base conversation events
    base_stream = "base-session"
    last_eid = None
    for i in range(10):
        last_eid = await store.store_event(base_stream, SAMPLE_MSG(i))

    assert last_eid is not None

    forked_streams = [f"fork-session-{i}" for i in range(num_forks)]

    # We will track active fork streams and verify histories on completion
    verification_errors = []

    # Fork all streams first so they exist and can be replayed safely
    for stream_name in forked_streams:
        await store.fork_stream(base_stream, last_eid, stream_name)

    async def fork_and_write_worker(stream_idx):
        try:
            stream_name = forked_streams[stream_idx]
            # Store events on this fork
            for step in range(writes_per_fork):
                val = 1000 + stream_idx * 100 + step
                await store.store_event(stream_name, SAMPLE_MSG(val))
                # Add a tiny sleep to simulate realistic network delay
                await asyncio.sleep(0.001)

        except Exception as e:
            verification_errors.append(f"Worker {stream_idx} failed: {e}")

    async def replay_worker():
        try:
            # Randomly select a stream and replay its history
            for _ in range(20):
                stream_name = random.choice(forked_streams)
                events = []

                async def cb(event: EventMessage) -> None:
                    events.append(event)

                await store.replay_events_after("0", cb, stream_name)

                # Each fork session must contain at least the 10 base events
                if len(events) < 10:
                    verification_errors.append(f"Replay on {stream_name} yielded too few events: {len(events)}")
                await asyncio.sleep(0.002)
        except Exception as e:
            verification_errors.append(f"Replay worker failed: {e}")

    # Start all fork/write workers and concurrent replay workers
    workers = [fork_and_write_worker(i) for i in range(num_forks)]
    replayers = [replay_worker() for _ in range(15)]

    start_time = time.monotonic()
    await asyncio.gather(*workers, *replayers)
    duration = time.monotonic() - start_time

    # Report performance stats
    total_operations = num_forks * writes_per_fork + 15 * 20
    throughput = total_operations / duration
    print(
        f"\n[Production Stress] Store: {store.__class__.__name__}, "
        f"Duration: {duration:.4f}s, Throughput: {throughput:.2f} ops/sec"
    )

    # Check for any exceptions or validation errors
    if verification_errors:
        pytest.fail(f"Stress test verification failed: {verification_errors}")

    # Verify final histories for each stream
    for idx, stream_name in enumerate(forked_streams):
        events = []

        async def cb(event: EventMessage) -> None:
            events.append(event)

        await store.replay_events_after("0", cb, stream_name)
        event_ids = [int(e.message.root.id) for e in events]

        # Verify shared prefix (0 to 9)
        assert event_ids[:10] == list(range(10))

        # Verify fork events
        expected_fork_ids = [1000 + idx * 100 + step for step in range(writes_per_fork)]
        assert event_ids[10:] == expected_fork_ids


@pytest.mark.anyio
async def test_production_stress_sqlite(sqlite_store):
    await run_production_stress_test(sqlite_store)


@pytest.mark.anyio
async def test_production_stress_redis(redis_store):
    await run_production_stress_test(redis_store)


# Conditional Postgres test
POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL")


@pytest.mark.skipif(POSTGRES_URL is None, reason="No Postgres URL configured")
@pytest.mark.anyio
async def test_production_stress_postgres():
    import asyncpg

    pool = await asyncpg.create_pool(POSTGRES_URL)
    try:
        await pool.execute("DROP TABLE IF EXISTS stress_fork_events")
        await pool.execute("DROP TABLE IF EXISTS stress_fork_events_forks")
        s = PostgresEventStore(pool, table_name="stress_fork_events", ttl=None)
        await s.initialize()
        await run_production_stress_test(s)
    finally:
        await pool.execute("DROP TABLE IF EXISTS stress_fork_events")
        await pool.execute("DROP TABLE IF EXISTS stress_fork_events_forks")
        await pool.close()
