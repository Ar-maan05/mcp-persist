# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportPrivateUsage=false
"""Tests for the create() async-context-manager convenience constructors.

These exercise the connection lifecycle without needing any external service:
Redis uses fakeredis, SQLite uses an in-memory database, and Postgres is driven
through a mocked asyncpg.create_pool, so the whole file runs locally.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import PostgresEventStore, RedisEventStore, SQLiteEventStore

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


async def _collect(store, anchor):
    captured: list[EventMessage] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    stream_id = await store.replay_events_after(anchor, cb)
    return captured, stream_id


# ── Redis ───────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_redis_create_yields_working_store_and_closes_client(monkeypatch):
    client = fakeredis.FakeRedis()
    closed = False

    async def tracking_aclose():
        nonlocal closed
        closed = True

    monkeypatch.setattr(client, "aclose", tracking_aclose, raising=False)

    import redis.asyncio as aioredis

    seen: dict = {}

    def fake_from_url(url, **kwargs):
        seen["url"] = url
        seen["kwargs"] = kwargs
        return client

    monkeypatch.setattr(aioredis, "from_url", fake_from_url)

    async with RedisEventStore.create(
        "redis://localhost:6379",
        key_prefix="test:",
        ttl=3600,
        decode_responses=True,
    ) as store:
        assert isinstance(store, RedisEventStore)
        anchor = await store.store_event("stream-A", None)
        event_id = await store.store_event("stream-A", SAMPLE_MSG)
        captured, stream_id = await _collect(store, anchor)
        assert stream_id == "stream-A"
        assert [e.event_id for e in captured] == [event_id]

    assert closed is True
    assert seen["url"] == "redis://localhost:6379"
    # Connection kwargs reach from_url; store kwargs (key_prefix, ttl) do not.
    assert seen["kwargs"] == {"decode_responses": True}


@pytest.mark.anyio
async def test_redis_create_closes_client_on_body_error(monkeypatch):
    client = fakeredis.FakeRedis()
    closed = False

    async def tracking_aclose():
        nonlocal closed
        closed = True

    monkeypatch.setattr(client, "aclose", tracking_aclose, raising=False)

    import redis.asyncio as aioredis

    monkeypatch.setattr(aioredis, "from_url", lambda url, **kwargs: client)

    with pytest.raises(RuntimeError, match="boom"):
        async with RedisEventStore.create("redis://x", ttl=1):
            raise RuntimeError("boom")

    assert closed is True


@pytest.mark.anyio
async def test_redis_create_falls_back_to_close_without_aclose(monkeypatch):
    """redis-py < 5.0 has no aclose(); create() must fall back to close()."""

    class _LegacyClient:
        def __init__(self) -> None:
            self.close_called = False

        async def close(self) -> None:
            self.close_called = True

    client = _LegacyClient()

    import redis.asyncio as aioredis

    monkeypatch.setattr(aioredis, "from_url", lambda url, **kwargs: client)

    async with RedisEventStore.create("redis://x", ttl=1) as store:
        assert isinstance(store, RedisEventStore)

    assert client.close_called is True


# ── SQLite ──────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_sqlite_create_yields_working_store_and_closes_connection():
    async with SQLiteEventStore.create(":memory:", table_name="ev", ttl=3600) as store:
        assert isinstance(store, SQLiteEventStore)
        conn = store._conn
        anchor = await store.store_event("stream-A", None)
        event_id = await store.store_event("stream-A", SAMPLE_MSG)
        captured, stream_id = await _collect(store, anchor)
        assert stream_id == "stream-A"
        assert [e.event_id for e in captured] == [event_id]

    # The connection is closed on exit: aiosqlite rejects use afterwards.
    with pytest.raises(ValueError):
        await conn.execute("SELECT 1")


@pytest.mark.anyio
async def test_sqlite_create_closes_connection_when_initialize_raises(monkeypatch):
    conn = AsyncMock()

    async def fake_connect(path, **kwargs):
        return conn

    monkeypatch.setattr(aiosqlite, "connect", fake_connect)
    monkeypatch.setattr(
        SQLiteEventStore,
        "initialize",
        AsyncMock(side_effect=RuntimeError("init failed")),
    )

    with pytest.raises(RuntimeError, match="init failed"):
        async with SQLiteEventStore.create("x.db", table_name="ev", ttl=1):
            pass

    conn.close.assert_awaited_once()


# ── Postgres ──────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_postgres_create_initializes_and_closes_pool(monkeypatch):
    import asyncpg

    pool = AsyncMock()
    captured: dict = {}

    async def fake_create_pool(dsn, **kwargs):
        captured["dsn"] = dsn
        captured["kwargs"] = kwargs
        return pool

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    init_mock = AsyncMock()
    monkeypatch.setattr(PostgresEventStore, "initialize", init_mock)

    async with PostgresEventStore.create(
        "postgresql://x",
        table_name="ev",
        ttl=3600,
        min_size=2,
    ) as store:
        assert isinstance(store, PostgresEventStore)

    init_mock.assert_awaited_once()
    pool.close.assert_awaited_once()
    assert captured["dsn"] == "postgresql://x"
    # Pool kwargs reach create_pool; store kwargs (table_name, ttl) do not.
    assert captured["kwargs"] == {"min_size": 2}


@pytest.mark.anyio
async def test_postgres_create_closes_pool_when_initialize_raises(monkeypatch):
    import asyncpg

    pool = AsyncMock()
    monkeypatch.setattr(asyncpg, "create_pool", AsyncMock(return_value=pool))
    monkeypatch.setattr(
        PostgresEventStore,
        "initialize",
        AsyncMock(side_effect=RuntimeError("init failed")),
    )

    with pytest.raises(RuntimeError, match="init failed"):
        async with PostgresEventStore.create("postgresql://x", ttl=1):
            pass

    pool.close.assert_awaited_once()
