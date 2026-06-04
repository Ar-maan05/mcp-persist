# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Stress tests for PostgresEventStore."""

from __future__ import annotations

import asyncio
import os
import resource
import time

import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import PostgresEventStore

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="stress", method="tools/list")
TABLE = "stress_events_thru"

POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL")

pytestmark = pytest.mark.skipif(
    POSTGRES_URL is None,
    reason="set MCP_TEST_POSTGRES_URL to run Postgres tests",
)


def get_memory_usage_kb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def get_open_fds() -> int:
    try:
        return len(os.listdir("/proc/self/fd"))
    except Exception:
        return 0


@pytest.fixture
async def pg_pool():
    import asyncpg

    pool = await asyncpg.create_pool(POSTGRES_URL)
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def clean_table(pg_pool):
    await pg_pool.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
    try:
        yield TABLE
    finally:
        await pg_pool.execute(f'DROP TABLE IF EXISTS "{TABLE}"')


@pytest.mark.anyio
async def test_postgres_high_throughput(pg_pool, clean_table):
    """Verify high-throughput write and read workloads on Postgres."""
    store = PostgresEventStore(pg_pool, table_name=clean_table, ttl=3600)
    await store.initialize()
    num_events = 2000
    stream_id = "high-thru-stream"

    # Write phase
    write_start = time.monotonic()
    for i in range(num_events):
        await store.store_event(stream_id, SAMPLE_MSG)
    write_duration = time.monotonic() - write_start
    write_throughput = num_events / write_duration
    avg_write_latency_ms = (write_duration / num_events) * 1000.0

    print(
        f"\n[Postgres Write] Duration: {write_duration:.4f}s, "
        f"Throughput: {write_throughput:.2f} ops/sec, "
        f"Avg Latency: {avg_write_latency_ms:.4f}ms"
    )

    # Read / Replay phase
    replayed: list[EventMessage] = []

    async def replay_cb(event: EventMessage):
        replayed.append(event)

    anchor = "1"  # Postgres IDENTITY column starts at 1

    replay_start = time.monotonic()
    await store.replay_events_after(anchor, replay_cb)
    replay_duration = time.monotonic() - replay_start
    replay_throughput = len(replayed) / replay_duration if replay_duration > 0 else 0
    avg_replay_latency_ms = (replay_duration / len(replayed)) * 1000.0 if replayed else 0

    print(
        f"[Postgres Replay] Replayed: {len(replayed)}, Duration: {replay_duration:.4f}s, "
        f"Throughput: {replay_throughput:.2f} ops/sec, "
        f"Avg Latency: {avg_replay_latency_ms:.4f}ms"
    )

    # The anchor event itself (1) is excluded from the replay, so we get num_events - 1
    assert len(replayed) == num_events - 1


@pytest.mark.anyio
async def test_postgres_concurrency(pg_pool, clean_table):
    """Verify concurrency limits: many concurrent writers/readers on Postgres."""
    store = PostgresEventStore(pg_pool, table_name=clean_table, ttl=3600)
    await store.initialize()

    num_writers = 15
    events_per_writer = 50

    async def writer_task(writer_id: int):
        for i in range(events_per_writer):
            await store.store_event(f"stream-{writer_id}", SAMPLE_MSG)
            await asyncio.sleep(0.001)

    async def reader_task():
        replayed: list[EventMessage] = []

        async def cb(event: EventMessage):
            replayed.append(event)

        for _ in range(5):
            await store.replay_events_after("1", cb)
            await asyncio.sleep(0.01)

    tasks = []
    for wid in range(num_writers):
        tasks.append(writer_task(wid))
    for rid in range(5):
        tasks.append(reader_task())

    start = time.monotonic()
    await asyncio.gather(*tasks)
    duration = time.monotonic() - start

    # Query total rows in DB
    val = await pg_pool.fetchval(f'SELECT COUNT(*) FROM "{clean_table}"')
    print(f"\n[Postgres Concurrency] Wrote {val} events across {num_writers} writers in {duration:.4f}s")
    assert val == num_writers * events_per_writer


@pytest.mark.anyio
async def test_postgres_resource_cleanup():
    """Verify system resource behavior under prolonged Postgres connection stress."""
    import asyncpg

    fds_before = get_open_fds()

    for _ in range(20):
        pool = await asyncpg.create_pool(POSTGRES_URL)
        store = PostgresEventStore(pool, table_name="cleanup_events", ttl=3600)
        await store.initialize()
        await store.store_event("clean-stream", SAMPLE_MSG)
        await pool.execute('DROP TABLE IF EXISTS "cleanup_events"')
        await pool.close()

    # Give time for socket closures
    await asyncio.sleep(0.2)

    fds_after = get_open_fds()
    fd_diff = fds_after - fds_before
    print(f"\n[Postgres Cleanup] FDs before: {fds_before}, FDs after: {fds_after}, Diff: {fd_diff}")

    # Verify no file descriptor leak
    assert fd_diff <= 3
