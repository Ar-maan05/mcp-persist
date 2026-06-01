# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportPrivateUsage=false
"""Tests for the push-based subscribe() streaming API.

Redis runs against fakeredis (real pub/sub semantics in-process) and SQLite uses
an in-memory database with the polling fallback, so the file needs no external
service. The Postgres LISTEN/NOTIFY path is covered for parity in
test_postgres_event_store.py (CI only).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.types import JSONRPCRequest

from mcp_persist import PostgresEventStore, RedisEventStore, SQLiteEventStore

# Give a subscriber time to register (SUBSCRIBE / first poll) before publishing.
REGISTER_DELAY = 0.2


def _msg(i: int) -> JSONRPCRequest:
    return JSONRPCRequest(jsonrpc="2.0", id=str(i), method="tools/call", params={"n": i})


def _id(message) -> str:
    return message.root.id


@pytest.fixture
async def redis_streaming_store():
    client = fakeredis.FakeRedis()
    store = RedisEventStore(client, key_prefix="strm:", ttl=None, enable_streaming=True)
    try:
        yield store
    finally:
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


@pytest.fixture
async def sqlite_streaming_store():
    conn = await aiosqlite.connect(":memory:")
    store = SQLiteEventStore(conn, table_name="ev", ttl=None, enable_streaming=True)
    await store.initialize()
    try:
        yield store
    finally:
        await conn.close()


def _consume(store, stream_id, n, received, **kwargs):
    """Start a background task collecting up to n events from subscribe()."""

    async def run():
        async for event_id, message in store.subscribe(stream_id, **kwargs):
            received.append((event_id, message))
            if len(received) >= n:
                break

    return asyncio.create_task(run())


# ── Redis ─────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_redis_subscribe_delivers_new_events(redis_streaming_store):
    received: list = []
    task = _consume(redis_streaming_store, "stream-A", 2, received)
    await asyncio.sleep(REGISTER_DELAY)

    await redis_streaming_store.store_event("stream-A", _msg(1))
    await redis_streaming_store.store_event("stream-A", _msg(2))

    await asyncio.wait_for(task, timeout=3)
    assert [_id(m) for _, m in received] == ["1", "2"]


@pytest.mark.anyio
async def test_redis_subscribe_is_forward_only(redis_streaming_store):
    # Written before the subscription exists — must NOT be delivered.
    await redis_streaming_store.store_event("stream-A", _msg(0))

    received: list = []
    task = _consume(redis_streaming_store, "stream-A", 1, received)
    await asyncio.sleep(REGISTER_DELAY)

    await redis_streaming_store.store_event("stream-A", _msg(1))

    await asyncio.wait_for(task, timeout=3)
    assert [_id(m) for _, m in received] == ["1"]


@pytest.mark.anyio
async def test_redis_subscribe_skips_priming_events(redis_streaming_store):
    received: list = []
    task = _consume(redis_streaming_store, "stream-A", 1, received)
    await asyncio.sleep(REGISTER_DELAY)

    await redis_streaming_store.store_event("stream-A", None)  # priming, not delivered
    await redis_streaming_store.store_event("stream-A", _msg(1))

    await asyncio.wait_for(task, timeout=3)
    assert [_id(m) for _, m in received] == ["1"]


@pytest.mark.anyio
async def test_redis_subscribe_only_its_own_stream(redis_streaming_store):
    received: list = []
    task = _consume(redis_streaming_store, "stream-A", 1, received)
    await asyncio.sleep(REGISTER_DELAY)

    await redis_streaming_store.store_event("stream-B", _msg(99))  # other stream
    await redis_streaming_store.store_event("stream-A", _msg(1))

    await asyncio.wait_for(task, timeout=3)
    assert [_id(m) for _, m in received] == ["1"]


@pytest.mark.anyio
async def test_redis_subscribe_cancellation_is_clean(redis_streaming_store):
    received: list = []
    task = _consume(redis_streaming_store, "stream-A", 99, received)
    await asyncio.sleep(REGISTER_DELAY)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The store is still usable afterwards (connection was released, not wedged).
    event_id = await redis_streaming_store.store_event("stream-A", _msg(1))
    assert isinstance(event_id, str)


@pytest.mark.anyio
async def test_redis_subscribe_requires_enable_streaming():
    client = fakeredis.FakeRedis()
    store = RedisEventStore(client, key_prefix="strm:", ttl=None)  # flag defaults False
    try:
        with pytest.raises(RuntimeError, match="enable_streaming"):
            async for _ in store.subscribe("stream-A"):
                break
    finally:
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


@pytest.mark.anyio
async def test_redis_store_event_unaffected_when_streaming_disabled():
    # With the default flag, store_event must not attempt any publish.
    client = fakeredis.FakeRedis()
    store = RedisEventStore(client, key_prefix="strm:", ttl=None)
    try:
        event_id = await store.store_event("stream-A", _msg(1))
        assert event_id == "1"
    finally:
        await client.aclose()


# ── SQLite ────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_sqlite_subscribe_delivers_new_events(sqlite_streaming_store):
    received: list = []
    task = _consume(sqlite_streaming_store, "stream-A", 2, received, poll_interval=0.05)
    await asyncio.sleep(REGISTER_DELAY)

    await sqlite_streaming_store.store_event("stream-A", _msg(1))
    await sqlite_streaming_store.store_event("stream-A", _msg(2))

    await asyncio.wait_for(task, timeout=3)
    assert [_id(m) for _, m in received] == ["1", "2"]


@pytest.mark.anyio
async def test_sqlite_subscribe_is_forward_only(sqlite_streaming_store):
    await sqlite_streaming_store.store_event("stream-A", _msg(0))  # before subscribe

    received: list = []
    task = _consume(sqlite_streaming_store, "stream-A", 1, received, poll_interval=0.05)
    await asyncio.sleep(REGISTER_DELAY)

    await sqlite_streaming_store.store_event("stream-A", _msg(1))

    await asyncio.wait_for(task, timeout=3)
    assert [_id(m) for _, m in received] == ["1"]


@pytest.mark.anyio
async def test_sqlite_subscribe_skips_priming_events(sqlite_streaming_store):
    received: list = []
    task = _consume(sqlite_streaming_store, "stream-A", 1, received, poll_interval=0.05)
    await asyncio.sleep(REGISTER_DELAY)

    await sqlite_streaming_store.store_event("stream-A", None)  # priming
    await sqlite_streaming_store.store_event("stream-A", _msg(1))

    await asyncio.wait_for(task, timeout=3)
    assert [_id(m) for _, m in received] == ["1"]


@pytest.mark.anyio
async def test_sqlite_subscribe_cancellation_is_clean(sqlite_streaming_store):
    received: list = []
    task = _consume(sqlite_streaming_store, "stream-A", 99, received, poll_interval=0.05)
    await asyncio.sleep(REGISTER_DELAY)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    event_id = await sqlite_streaming_store.store_event("stream-A", _msg(1))
    assert isinstance(event_id, str)


@pytest.mark.anyio
async def test_sqlite_subscribe_requires_enable_streaming():
    conn = await aiosqlite.connect(":memory:")
    store = SQLiteEventStore(conn, table_name="ev", ttl=None)  # flag defaults False
    await store.initialize()
    try:
        with pytest.raises(RuntimeError, match="enable_streaming"):
            async for _ in store.subscribe("stream-A"):
                break
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_sqlite_subscribe_rejects_non_positive_poll_interval(sqlite_streaming_store):
    with pytest.raises(ValueError, match="poll_interval"):
        async for _ in sqlite_streaming_store.subscribe("stream-A", poll_interval=0):
            break


# ── Connection-release on teardown (regression: cleanup must not leak) ─────────


@pytest.mark.anyio
async def test_redis_subscribe_closes_pubsub_even_if_unsubscribe_raises():
    """unsubscribe() raising CancelledError on teardown must not skip the close."""
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    # Simulate cancellation landing inside unsubscribe during teardown.
    pubsub.unsubscribe = AsyncMock(side_effect=asyncio.CancelledError())
    pubsub.aclose = AsyncMock()

    async def blocking_listen():
        await asyncio.Event().wait()
        yield  # pragma: no cover - never reached; makes this an async generator

    pubsub.listen = blocking_listen

    client = MagicMock()
    client.pubsub = MagicMock(return_value=pubsub)

    store = RedisEventStore(client, key_prefix="strm:", ttl=None, enable_streaming=True)

    async def run():
        async for _ in store.subscribe("stream-A"):
            pass

    task = asyncio.create_task(run())
    await asyncio.sleep(0.1)  # let it reach pubsub.listen()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    pubsub.aclose.assert_awaited_once()


@pytest.mark.anyio
async def test_postgres_subscribe_releases_connection_even_if_remove_listener_raises():
    """remove_listener() raising CancelledError on teardown must not skip release."""
    conn = AsyncMock()
    conn.remove_listener = AsyncMock(side_effect=asyncio.CancelledError())

    pool = MagicMock()
    pool.acquire = AsyncMock(return_value=conn)
    pool.release = AsyncMock()

    store = PostgresEventStore(pool, table_name="ev", ttl=None, enable_streaming=True)
    store._initialized = True  # skip DDL; pool is a mock

    async def run():
        async for _ in store.subscribe("stream-A"):
            pass

    task = asyncio.create_task(run())
    await asyncio.sleep(0.1)  # let it reach queue.get()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    pool.release.assert_awaited_once_with(conn)
