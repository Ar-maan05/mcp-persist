# pyright: reportPrivateUsage=false
"""Tests for tiered storage: archive batch, ArchiveScheduler, ChainedEventStore."""

from __future__ import annotations

import asyncio
import time

import aiosqlite
import pytest
from mcp.types import JSONRPCRequest

from mcp_persist import (
    ArchiveScheduler,
    ChainedEventStore,
    SQLiteEventStore,
    archive_expired_batch,
    count_expired,
)

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


@pytest.fixture
async def hot_cold_stores():
    hot_conn = await aiosqlite.connect(":memory:")
    cold_conn = await aiosqlite.connect(":memory:")
    hot = SQLiteEventStore(hot_conn, table_name="hot", ttl=60)
    cold = SQLiteEventStore(cold_conn, table_name="cold", ttl=None)
    await hot.initialize()
    await cold.initialize()
    try:
        yield hot, cold
    finally:
        await hot_conn.close()
        await cold_conn.close()


@pytest.mark.anyio
async def test_archive_expired_batch_moves_and_preserves_ids(hot_cold_stores):
    hot, cold = hot_cold_stores
    priming = await hot.store_event("stream-a", None)
    event_id = await hot.store_event("stream-a", SAMPLE_MSG)
    await hot_conn_set_old(hot, event_id)
    await hot_conn_set_old(hot, priming)

    archived = await archive_expired_batch(hot, cold, batch_size=10)
    assert archived == 2

    assert not await hot._event_exists(event_id)
    assert await cold._event_exists(event_id)

    replayed: list[str] = []

    async def cb(event):
        replayed.append(event.event_id)

    chain = ChainedEventStore(hot=hot, cold=cold)
    await chain.replay_events_after(priming, cb)
    assert event_id in replayed


async def hot_conn_set_old(store: SQLiteEventStore, event_id: str) -> None:
    await store._conn.execute(  # type: ignore[attr-defined]
        "UPDATE hot SET created_at = ? WHERE event_id = ?",
        (time.time() - 120, int(event_id)),
    )
    await store._conn.commit()  # type: ignore[attr-defined]


@pytest.mark.anyio
async def test_count_expired_dry_run(hot_cold_stores):
    hot, cold = hot_cold_stores
    event_id = await hot.store_event("s", SAMPLE_MSG)
    await hot_conn_set_old(hot, event_id)
    assert await count_expired(hot) == 1
    assert await hot._event_exists(event_id)


@pytest.mark.anyio
async def test_chained_replay_from_hot_when_anchor_present(hot_cold_stores):
    hot, cold = hot_cold_stores
    e1 = await hot.store_event("s", SAMPLE_MSG)
    e2 = await hot.store_event("s", SAMPLE_MSG)
    chain = ChainedEventStore(hot=hot, cold=cold)
    seen: list[str] = []

    async def cb(event):
        seen.append(event.event_id)

    await chain.replay_events_after(e1, cb)
    assert seen == [e2]


@pytest.mark.anyio
async def test_archive_scheduler_runs(hot_cold_stores):
    hot, cold = hot_cold_stores
    event_id = await hot.store_event("s", SAMPLE_MSG)
    await hot_conn_set_old(hot, event_id)
    async with ArchiveScheduler(hot, cold, interval=0.02, batch_size=10):
        for _ in range(50):
            if await cold._event_exists(event_id):
                break
            await asyncio.sleep(0.02)
    assert await cold._event_exists(event_id)
