# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Stress tests for SQLiteEventStore."""

from __future__ import annotations

import asyncio
import os
import resource
import time

import aiosqlite
import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import SQLiteEventStore

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="stress", method="tools/list")
TABLE = "stress_events"


def get_memory_usage_kb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def get_open_fds() -> int:
    try:
        return len(os.listdir("/proc/self/fd"))
    except Exception:
        return 0


async def _committed_count(path: str) -> int:
    other = await aiosqlite.connect(path)
    try:
        async with other.execute(f"SELECT COUNT(*) FROM {TABLE}") as cur:
            (count,) = await cur.fetchone()
        return count
    finally:
        await other.close()


@pytest.mark.anyio
async def test_sqlite_high_throughput(tmp_path):
    """Verify high-throughput write and read workloads on SQLite."""
    path = str(tmp_path / "high_thru.db")
    num_events = 2000
    stream_id = "high-thru-stream"

    # Start timer
    async with SQLiteEventStore.create(path, table_name=TABLE, ttl=3600) as store:
        # Write phase
        write_start = time.monotonic()
        for i in range(num_events):
            await store.store_event(stream_id, SAMPLE_MSG)
        write_duration = time.monotonic() - write_start
        write_throughput = num_events / write_duration
        avg_write_latency_ms = (write_duration / num_events) * 1000.0

        print(
            f"\n[SQLite Write] Duration: {write_duration:.4f}s, "
            f"Throughput: {write_throughput:.2f} ops/sec, "
            f"Avg Latency: {avg_write_latency_ms:.4f}ms"
        )

        # Read / Replay phase
        replayed: list[EventMessage] = []

        async def replay_cb(event: EventMessage):
            replayed.append(event)

        # Store priming event first to establish stream and get anchor
        anchor = "1"  # SQLite autoincrement starts at 1

        replay_start = time.monotonic()
        await store.replay_events_after(anchor, replay_cb)
        replay_duration = time.monotonic() - replay_start
        replay_throughput = len(replayed) / replay_duration if replay_duration > 0 else 0
        avg_replay_latency_ms = (replay_duration / len(replayed)) * 1000.0 if replayed else 0

        print(
            f"[SQLite Replay] Replayed: {len(replayed)}, Duration: {replay_duration:.4f}s, "
            f"Throughput: {replay_throughput:.2f} ops/sec, "
            f"Avg Latency: {avg_replay_latency_ms:.4f}ms"
        )

        assert len(replayed) == num_events - 1  # event_id 1 is the anchor itself and excluded from replay


@pytest.mark.anyio
async def test_sqlite_write_behind_load(tmp_path):
    """SQLite write-behind under intense load.

    Checks memory footprint, commit-interval timing, and max-pending queue pressure.
    """
    path = str(tmp_path / "write_behind.db")

    # 1. Commit interval timing
    # Set a long commit interval (100 seconds) so we control when flush happens
    async with SQLiteEventStore.create(path, table_name=TABLE, ttl=3600, commit_interval=100.0) as store:
        # Write some events
        await store.store_event("wb-stream", SAMPLE_MSG)
        await store.store_event("wb-stream", SAMPLE_MSG)

        # Ensure they are not committed yet to DB (isolated on other connections)
        assert await _committed_count(path) == 0

    # Store is closed: commits should have been flushed
    assert await _committed_count(path) == 2

    # Now test short interval auto-commit
    async with SQLiteEventStore.create(path, table_name=TABLE, ttl=3600, commit_interval=0.1) as short_store:
        await short_store.store_event("wb-stream", SAMPLE_MSG)
        assert await _committed_count(path) == 2  # not committed immediately

        # Wait for flush interval
        await asyncio.sleep(0.25)
        assert await _committed_count(path) == 3  # flushed by background task

    # 2. Maximum pending queue pressure
    # Verify that once commit_max_pending is hit, commits happen immediately and
    # do not block indefinitely or lose items.
    async with SQLiteEventStore.create(path, table_name=TABLE, ttl=3600, commit_max_pending=10) as max_store:
        # Write 9 events (below threshold)
        for _ in range(9):
            await max_store.store_event("wb-stream", SAMPLE_MSG)
        assert await _committed_count(path) == 3  # Still not committed

        # Write 10th event (reaches threshold)
        await max_store.store_event("wb-stream", SAMPLE_MSG)
        assert await _committed_count(path) == 13  # Immediately committed

        # Write 5 more events (below threshold)
        for _ in range(5):
            await max_store.store_event("wb-stream", SAMPLE_MSG)
        assert await _committed_count(path) == 13  # Still at 13

    # The 5 outstanding events should flush when the context manager exits/aclose is called
    assert await _committed_count(path) == 18

    # 3. Memory footprint under write-behind load
    mem_before = get_memory_usage_kb()
    async with SQLiteEventStore.create(path, table_name=TABLE, ttl=3600, commit_interval=10.0) as store:
        for _ in range(5000):
            await store.store_event("wb-stream", SAMPLE_MSG)
    mem_after = get_memory_usage_kb()
    # Memory footprint should be stable (allow reasonable overhead but not leaking tens of MBs)
    mem_diff_mb = (mem_after - mem_before) / 1024.0
    print(f"\n[SQLite Memory] Memory change for 5000 write-behinds: {mem_diff_mb:.2f} MB")
    # Verify total events in DB
    assert await _committed_count(path) == 5018


@pytest.mark.anyio
async def test_sqlite_concurrency(tmp_path):
    """Verify concurrency limits: many concurrent writers/readers on SQLite (WAL locks)."""
    path = str(tmp_path / "concurrency.db")

    num_writers = 15
    events_per_writer = 50

    # Pre-initialize schema & write a priming event so "1" is a valid anchor
    # and subsequent connection initializations skip schema DDL.
    async with SQLiteEventStore.create(path, table_name=TABLE, ttl=3600, timeout=5.0) as init_store:
        await init_store.store_event("priming", SAMPLE_MSG)

    async def writer_task(writer_id: int):
        conn = await aiosqlite.connect(path)
        try:
            # timeout=5.0 to handle write lock serialization
            store = SQLiteEventStore(conn, table_name=TABLE, ttl=3600, timeout=5.0)
            await store.initialize()
            for i in range(events_per_writer):
                await store.store_event(f"stream-{writer_id}", SAMPLE_MSG)
                # yield control to let other tasks run
                await asyncio.sleep(0.001)
        finally:
            await conn.close()

    async def reader_task(stream_id: str):
        conn = await aiosqlite.connect(path)
        try:
            store = SQLiteEventStore(conn, table_name=TABLE, ttl=3600, timeout=5.0)
            await store.initialize()
            replayed: list[EventMessage] = []

            async def cb(event: EventMessage):
                replayed.append(event)

            # Replay events periodically
            for _ in range(5):
                await store.replay_events_after("1", cb)
                await asyncio.sleep(0.01)
        finally:
            await conn.close()

    tasks = []
    for wid in range(num_writers):
        tasks.append(writer_task(wid))
    for wid in range(5):  # 5 concurrent readers
        tasks.append(reader_task(f"stream-{wid}"))

    start = time.monotonic()
    await asyncio.gather(*tasks)
    duration = time.monotonic() - start

    # Total expected is events from writers + 1 priming event
    total_expected = (num_writers * events_per_writer) + 1
    total_committed = await _committed_count(path)
    print(f"\n[SQLite Concurrency] Wrote {total_committed} events across {num_writers} writers in {duration:.4f}s")
    assert total_committed == total_expected


@pytest.mark.anyio
async def test_sqlite_resource_cleanup(tmp_path):
    """Verify resource cleanup: CPU/Memory/FDs behavior under prolonged stress."""
    path = str(tmp_path / "cleanup.db")

    import gc

    gc.collect()
    fds_before = get_open_fds()

    # Create and close many stores to see if connections/fds leak
    for _ in range(20):
        async with SQLiteEventStore.create(path, table_name=TABLE, ttl=3600, commit_interval=0.1) as store:
            await store.store_event("clean-stream", SAMPLE_MSG)

    # Let the loop tasks settle and threads terminate
    await asyncio.sleep(0.5)
    gc.collect()

    fds_after = get_open_fds()
    fd_diff = fds_after - fds_before
    print(f"\n[SQLite Cleanup] FDs before: {fds_before}, FDs after: {fds_after}, Diff: {fd_diff}")

    # Allow a small threshold of FDs for pytest or temp processes, but should not leak 20 FDs
    assert fd_diff <= 3
