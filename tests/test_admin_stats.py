"""Tests for the ``mcp-persist stats`` command.

The counts are gathered against real stores (an in-memory SQLite database and
fakeredis), since the per-backend aggregate queries are the part worth pinning
down. The renderers are covered separately with hand-built reports.
"""

from __future__ import annotations

import json

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.types import JSONRPCRequest

from mcp_persist import RedisEventStore, SQLiteEventStore
from mcp_persist._admin import (
    StatsReport,
    StoreConfig,
    StreamStat,
    _render_stats,
    _render_stats_json,
    gather_stats,
)

MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


# SQLite (real aiosqlite)


@pytest.fixture
async def sqlite_store():
    conn = await aiosqlite.connect(":memory:")
    store = SQLiteEventStore(conn, table_name="events", ttl=None)
    await store.initialize()
    try:
        yield store
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_sqlite_stats_counts_per_stream(sqlite_store):
    await sqlite_store.store_event("stream-a", MSG)
    await sqlite_store.store_event("stream-a", MSG)
    await sqlite_store.store_event("stream-b", MSG)
    cfg = StoreConfig(backend="sqlite", url=":memory:")

    report = await gather_stats(cfg, sqlite_store)

    assert report.total_streams == 2
    assert report.total_events == 3
    assert report.last_event_id == 3
    by_id = {s.stream_id: s for s in report.streams}
    assert (by_id["stream-a"].events, by_id["stream-a"].min_event_id, by_id["stream-a"].max_event_id) == (2, 1, 2)
    assert (by_id["stream-b"].events, by_id["stream-b"].min_event_id, by_id["stream-b"].max_event_id) == (1, 3, 3)


@pytest.mark.anyio
async def test_sqlite_stats_streams_are_sorted(sqlite_store):
    await sqlite_store.store_event("zeta", MSG)
    await sqlite_store.store_event("alpha", MSG)
    report = await gather_stats(StoreConfig(backend="sqlite", url=":memory:"), sqlite_store)
    assert [s.stream_id for s in report.streams] == ["alpha", "zeta"]


@pytest.mark.anyio
async def test_sqlite_stats_stream_filter(sqlite_store):
    await sqlite_store.store_event("stream-a", MSG)
    await sqlite_store.store_event("stream-b", MSG)
    report = await gather_stats(StoreConfig(backend="sqlite", url=":memory:"), sqlite_store, stream_id="stream-a")
    assert [s.stream_id for s in report.streams] == ["stream-a"]
    assert report.total_events == 1


@pytest.mark.anyio
async def test_sqlite_stats_filter_missing_stream_is_zero_row(sqlite_store):
    await sqlite_store.store_event("stream-a", MSG)
    report = await gather_stats(StoreConfig(backend="sqlite", url=":memory:"), sqlite_store, stream_id="ghost")
    assert len(report.streams) == 1
    assert report.streams[0].events == 0
    assert report.streams[0].min_event_id is None
    assert report.total_streams == 0  # a zero row does not count toward streams-with-events


@pytest.mark.anyio
async def test_sqlite_stats_empty_store(sqlite_store):
    report = await gather_stats(StoreConfig(backend="sqlite", url=":memory:"), sqlite_store)
    assert report.streams == []
    assert (report.total_streams, report.total_events, report.last_event_id) == (0, 0, None)
    assert report.latency_ms >= 0.0


# Redis (fakeredis)


@pytest.fixture
async def redis_store():
    client = fakeredis.FakeRedis()
    store = RedisEventStore(client, key_prefix="mcp:", ttl=None)
    try:
        yield store
    finally:
        await client.flushall()
        await client.aclose()


@pytest.mark.anyio
async def test_redis_stats_counts_and_counter(redis_store):
    await redis_store.store_event("stream-a", MSG)
    await redis_store.store_event("stream-b", MSG)
    await redis_store.store_event("stream-a", MSG)
    report = await gather_stats(StoreConfig(backend="redis", url="redis://x"), redis_store)

    assert report.total_streams == 2
    assert report.total_events == 3
    assert report.last_event_id == 3  # never-expired counter
    by_id = {s.stream_id: s for s in report.streams}
    assert (by_id["stream-a"].events, by_id["stream-a"].min_event_id, by_id["stream-a"].max_event_id) == (2, 1, 3)
    assert by_id["stream-b"].events == 1


@pytest.mark.anyio
async def test_redis_stats_stream_filter(redis_store):
    await redis_store.store_event("stream-a", MSG)
    await redis_store.store_event("stream-b", MSG)
    report = await gather_stats(StoreConfig(backend="redis", url="redis://x"), redis_store, stream_id="stream-b")
    assert [s.stream_id for s in report.streams] == ["stream-b"]
    assert report.streams[0].events == 1


@pytest.mark.anyio
async def test_redis_stats_empty_store(redis_store):
    report = await gather_stats(StoreConfig(backend="redis", url="redis://x"), redis_store)
    assert report.streams == []
    assert (report.total_streams, report.total_events, report.last_event_id) == (0, 0, None)


# Rendering


def _report() -> StatsReport:
    return StatsReport(
        backend="sqlite",
        streams=[StreamStat("alpha", 12, 1, 12), StreamStat("beta", 5, 13, 17)],
        total_events=17,
        total_streams=2,
        last_event_id=17,
        latency_ms=0.4213,
    )


def test_render_stats_table_has_rows_and_summary():
    out = _render_stats(StoreConfig(backend="sqlite", url="e.db"), _report())
    assert "alpha" in out and "beta" in out
    assert "2 stream(s), 17 event(s), last id 17" in out
    assert "ping 0.42 ms" in out


def test_render_stats_empty():
    empty = StatsReport(
        backend="sqlite", streams=[], total_events=0, total_streams=0, last_event_id=None, latency_ms=0.1
    )
    out = _render_stats(StoreConfig(backend="sqlite", url="e.db"), empty)
    assert "no streams stored" in out
    assert "last id -" in out


def test_render_stats_json_round_trips():
    payload = json.loads(_render_stats_json(StoreConfig(backend="sqlite", url="e.db"), _report()))
    assert payload["total_events"] == 17
    assert payload["last_event_id"] == 17
    assert payload["latency_ms"] == 0.421
    assert [s["stream_id"] for s in payload["streams"]] == ["alpha", "beta"]
