# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Stress tests for RedisEventStore."""

from __future__ import annotations

import asyncio
import os
import resource
import time

import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import RedisEventStore

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="stress", method="tools/list")
REAL_REDIS_URL = os.environ.get("MCP_TEST_REDIS_URL")


def get_memory_usage_kb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def get_open_fds() -> int:
    try:
        return len(os.listdir("/proc/self/fd"))
    except Exception:
        return 0


@pytest.fixture
async def redis_client():
    if REAL_REDIS_URL:
        import redis.asyncio as real_redis

        client = real_redis.from_url(REAL_REDIS_URL)
        await client.flushdb()
    else:
        import fakeredis.aioredis as fakeredis

        client = fakeredis.FakeRedis()

    try:
        yield client
    finally:
        if REAL_REDIS_URL:
            await client.flushdb()
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


@pytest.mark.anyio
async def test_redis_high_throughput(redis_client):
    """Verify high-throughput write and read workloads on Redis."""
    store = RedisEventStore(redis_client, key_prefix="stress:", ttl=3600)
    num_events = 2000
    stream_id = "high-thru-stream"

    # Write phase
    write_start = time.monotonic()
    for i in range(num_events):
        await store.store_event(stream_id, SAMPLE_MSG)
    write_duration = time.monotonic() - write_start
    write_throughput = num_events / write_duration
    avg_write_latency_ms = (write_duration / num_events) * 1000.0

    backend_type = "Real Redis" if REAL_REDIS_URL else "fakeredis"
    print(
        f"\n[{backend_type} Write] Duration: {write_duration:.4f}s, "
        f"Throughput: {write_throughput:.2f} ops/sec, "
        f"Avg Latency: {avg_write_latency_ms:.4f}ms"
    )

    # Read / Replay phase
    replayed: list[EventMessage] = []

    async def replay_cb(event: EventMessage):
        replayed.append(event)

    anchor = "1"  # Redis counter starts at 1 for the first store_event

    replay_start = time.monotonic()
    await store.replay_events_after(anchor, replay_cb)
    replay_duration = time.monotonic() - replay_start
    replay_throughput = len(replayed) / replay_duration if replay_duration > 0 else 0
    avg_replay_latency_ms = (replay_duration / len(replayed)) * 1000.0 if replayed else 0

    print(
        f"[{backend_type} Replay] Replayed: {len(replayed)}, Duration: {replay_duration:.4f}s, "
        f"Throughput: {replay_throughput:.2f} ops/sec, "
        f"Avg Latency: {avg_replay_latency_ms:.4f}ms"
    )

    # The anchor event itself (1) is excluded from the replay, so we get num_events - 1
    assert len(replayed) == num_events - 1


@pytest.mark.anyio
async def test_redis_concurrency(redis_client):
    """Verify concurrency limits: many concurrent writers/readers on Redis."""
    # Prefix to isolate keys
    prefix = "stress-concurrent:"
    store = RedisEventStore(redis_client, key_prefix=prefix, ttl=3600)

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

    # Query total keys to verify all written
    # We can inspect the counter
    counter_key = f"{prefix}counter"
    val = await redis_client.get(counter_key)
    total_written = int(val) if val else 0
    backend_type = "Real Redis" if REAL_REDIS_URL else "fakeredis"
    print(
        f"\n[{backend_type} Concurrency] Wrote {total_written} events across {num_writers} writers in {duration:.4f}s"
    )
    assert total_written == num_writers * events_per_writer


@pytest.mark.anyio
async def test_redis_resource_cleanup():
    """Verify system resource behavior under prolonged Redis connection stress."""
    if not REAL_REDIS_URL:
        pytest.skip("Resource cleanup using real FDs requires a real Redis server URL")

    import redis.asyncio as real_redis

    fds_before = get_open_fds()

    for _ in range(20):
        client = real_redis.from_url(REAL_REDIS_URL)
        store = RedisEventStore(client, key_prefix="cleanup:", ttl=3600)
        await store.store_event("clean-stream", SAMPLE_MSG)
        try:
            await client.aclose()
        except AttributeError:
            await client.close()

    # Give time for socket closures
    await asyncio.sleep(0.2)

    fds_after = get_open_fds()
    fd_diff = fds_after - fds_before
    print(f"\n[Redis Cleanup] FDs before: {fds_before}, FDs after: {fds_after}, Diff: {fd_diff}")

    # Verify no file descriptor leak
    assert fd_diff <= 3
