# pyright: reportPrivateUsage=false
# pyright: reportArgumentType=false
# pyright: reportMissingImports=false
"""Multi-tenancy isolation tests.

A tenant-bound store must scope every read (replay, list_streams, purge,
count_expired) to its own rows; an unbound store sees every tenant. Covers
SQLite (shared table, tenant_id column) and Redis (key prefix) without a server.
"""

from __future__ import annotations

import time

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import RedisEventStore, SQLiteEventStore

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


async def _replay(store, last_event_id):
    captured: list[EventMessage] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    sid = await store.replay_events_after(last_event_id, cb)
    return sid, captured


# ── SQLite: two tenants share one table ───────────────────────────────────────


@pytest.fixture
async def sqlite_conn():
    conn = await aiosqlite.connect(":memory:")
    try:
        yield conn
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_sqlite_replay_does_not_cross_tenants(sqlite_conn):
    acme = SQLiteEventStore(sqlite_conn, table_name="ev", tenant_id="acme", ttl=None)
    globex = SQLiteEventStore(sqlite_conn, table_name="ev", tenant_id="globex", ttl=None)
    await acme.initialize()
    await globex.initialize()

    a_first = await acme.store_event("stream-1", None)
    await acme.store_event("stream-1", SAMPLE_MSG)
    b_anchor = await globex.store_event("stream-1", None)
    await globex.store_event("stream-1", SAMPLE_MSG)

    # globex resuming from acme's event id must resolve nothing (different tenant).
    sid, captured = await _replay(globex, a_first)
    assert sid is None
    assert captured == []

    # globex resuming from its own anchor sees only its own event.
    sid, captured = await _replay(globex, b_anchor)
    assert sid == "stream-1"
    assert len(captured) == 1


@pytest.mark.anyio
async def test_sqlite_list_streams_scoped(sqlite_conn):
    acme = SQLiteEventStore(sqlite_conn, table_name="ev", tenant_id="acme", ttl=None)
    globex = SQLiteEventStore(sqlite_conn, table_name="ev", tenant_id="globex", ttl=None)
    unscoped = SQLiteEventStore(sqlite_conn, table_name="ev", tenant_id=None, ttl=None)
    for s in (acme, globex, unscoped):
        await s.initialize()

    await acme.store_event("a-stream", SAMPLE_MSG)
    await globex.store_event("g-stream", SAMPLE_MSG)

    assert {s async for s in acme.list_streams()} == {"a-stream"}
    assert {s async for s in globex.list_streams()} == {"g-stream"}
    # An unbound (admin) store sees every tenant's streams.
    assert {s async for s in unscoped.list_streams()} == {"a-stream", "g-stream"}


@pytest.mark.anyio
async def test_sqlite_purge_and_count_scoped(sqlite_conn):
    acme = SQLiteEventStore(sqlite_conn, table_name="ev", tenant_id="acme", ttl=1)
    globex = SQLiteEventStore(sqlite_conn, table_name="ev", tenant_id="globex", ttl=1)
    await acme.initialize()
    await globex.initialize()

    await acme.store_event("s", SAMPLE_MSG)
    await globex.store_event("s", SAMPLE_MSG)
    # Age every row past the 1s ttl.
    await sqlite_conn.execute("UPDATE ev SET created_at = ?", (time.time() - 3600,))
    await sqlite_conn.commit()

    assert await acme.count_expired() == 1  # only acme's row
    purged = await acme.purge_expired()
    assert purged == 1
    # globex's expired row is untouched by acme's purge.
    assert await globex.count_expired() == 1


# ── Redis: two tenants share one server via key prefix ────────────────────────


@pytest.fixture
async def redis_client():
    client = fakeredis.FakeRedis()
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


@pytest.mark.anyio
async def test_redis_replay_does_not_cross_tenants(redis_client):
    acme = RedisEventStore(redis_client, tenant_id="acme", ttl=3600)
    globex = RedisEventStore(redis_client, tenant_id="globex", ttl=3600)

    a_anchor = await acme.store_event("stream-1", None)
    await acme.store_event("stream-1", SAMPLE_MSG)

    # globex cannot resolve acme's anchor: it lives under a different key prefix.
    sid, captured = await _replay(globex, a_anchor)
    assert sid is None
    assert captured == []
