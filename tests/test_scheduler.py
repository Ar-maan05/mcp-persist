# pyright: reportPrivateUsage=false
"""Tests for PurgeScheduler.

The scheduler is backend-agnostic, so most tests drive it with a tiny in-process
fake store; one end-to-end test runs it against a real (in-memory) SQLite store.
All tests are async (anyio/asyncio backend).
"""

from __future__ import annotations

import asyncio
import time

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.types import JSONRPCRequest

from mcp_persist import PurgeScheduler, RedisEventStore, SQLiteEventStore

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


class FakeStore:
    def __init__(self, to_remove: int = 0) -> None:
        self.calls = 0
        self.batch_sizes: list[int | None] = []
        self.to_remove = to_remove

    async def purge_expired(self, *, batch_size: int | None = None) -> int:
        self.calls += 1
        self.batch_sizes.append(batch_size)
        return self.to_remove


class FailingStore:
    def __init__(self) -> None:
        self.calls = 0

    async def purge_expired(self, *, batch_size: int | None = None) -> int:
        self.calls += 1
        raise RuntimeError("boom")


@pytest.mark.anyio
async def test_scheduler_calls_purge_periodically():
    store = FakeStore()
    scheduler = PurgeScheduler(store, interval=0.02)
    await scheduler.start()
    await asyncio.sleep(0.1)
    await scheduler.aclose()
    assert store.calls >= 2


@pytest.mark.anyio
async def test_scheduler_context_manager():
    store = FakeStore()
    async with PurgeScheduler(store, interval=0.02):
        await asyncio.sleep(0.1)
    assert store.calls >= 2


@pytest.mark.anyio
async def test_scheduler_forwards_batch_size():
    store = FakeStore()
    async with PurgeScheduler(store, interval=0.02, batch_size=7):
        await asyncio.sleep(0.08)
    assert store.batch_sizes
    assert all(b == 7 for b in store.batch_sizes)


@pytest.mark.anyio
async def test_scheduler_default_batch_size_is_none():
    store = FakeStore()
    async with PurgeScheduler(store, interval=0.02):
        await asyncio.sleep(0.08)
    assert all(b is None for b in store.batch_sizes)


@pytest.mark.anyio
async def test_scheduler_survives_purge_errors():
    store = FailingStore()
    async with PurgeScheduler(store, interval=0.02):
        await asyncio.sleep(0.1)
    # The loop kept firing despite every call raising.
    assert store.calls >= 2


@pytest.mark.anyio
async def test_scheduler_double_start_raises():
    scheduler = PurgeScheduler(FakeStore(), interval=0.05)
    await scheduler.start()
    try:
        with pytest.raises(RuntimeError):
            await scheduler.start()
    finally:
        await scheduler.aclose()


@pytest.mark.anyio
async def test_scheduler_aclose_without_start_is_safe():
    scheduler = PurgeScheduler(FakeStore(), interval=0.05)
    await scheduler.aclose()  # must not raise


def test_scheduler_rejects_store_without_purge():
    with pytest.raises(TypeError):
        PurgeScheduler(object(), interval=1.0)


def test_scheduler_rejects_redis_store():
    client = fakeredis.FakeRedis()
    with pytest.raises(TypeError):
        PurgeScheduler(RedisEventStore(client, ttl=60), interval=1.0)


def test_scheduler_invalid_interval_raises():
    with pytest.raises(ValueError):
        PurgeScheduler(FakeStore(), interval=0)


def test_scheduler_invalid_batch_size_raises():
    with pytest.raises(ValueError):
        PurgeScheduler(FakeStore(), interval=1.0, batch_size=0)


def test_scheduler_invalid_jitter_raises():
    with pytest.raises(ValueError):
        PurgeScheduler(FakeStore(), interval=1.0, jitter=-0.5)


@pytest.mark.anyio
async def test_scheduler_fires_with_jitter():
    # A small interval plus a small jitter window: the loop must still fire
    # repeatedly, exercising the jittered-sleep path.
    store = FakeStore()
    async with PurgeScheduler(store, interval=0.02, jitter=0.02):
        await asyncio.sleep(0.2)
    assert store.calls >= 2


@pytest.mark.anyio
async def test_scheduler_purges_real_sqlite_store():
    conn = await aiosqlite.connect(":memory:")
    try:
        store = SQLiteEventStore(conn, table_name="sched_events", ttl=60)
        await store.initialize()
        event_id = await store.store_event("s", SAMPLE_MSG)
        await conn.execute(
            "UPDATE sched_events SET created_at = ? WHERE event_id = ?",
            (time.time() - 120, int(event_id)),
        )
        await conn.commit()

        count = 1
        async with PurgeScheduler(store, interval=0.02):
            for _ in range(100):
                async with conn.execute("SELECT COUNT(*) FROM sched_events") as cur:
                    (count,) = await cur.fetchone()
                if count == 0:
                    break
                await asyncio.sleep(0.02)
        assert count == 0
    finally:
        await conn.close()
