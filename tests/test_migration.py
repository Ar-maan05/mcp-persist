# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportPrivateUsage=false
"""Tests for the cross-backend migrate() utility.

SQLite-to-SQLite uses two independent in-memory databases (separate connections),
and a cross-backend case migrates SQLite-to-Redis and Redis-to-SQLite via
fakeredis, so the whole file runs without an external service.
"""

from __future__ import annotations

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.types import JSONRPCRequest

from mcp_persist import MigrationResult, RedisEventStore, SQLiteEventStore, migrate


def _msg(i: int) -> JSONRPCRequest:
    """A distinct, identifiable message so ordering can be checked."""
    return JSONRPCRequest(jsonrpc="2.0", id=str(i), method="tools/call", params={"n": i})


async def _dump(store, stream_id) -> list[str | None]:
    """Serialize every event on a stream (oldest first) for comparison.

    Both source and destination are dumped through the same path so the
    comparison is apples-to-apples despite IDs differing across backends.
    """
    out: list[str | None] = []
    async for _event_id, message in store._iter_stream_events(stream_id):
        out.append(None if message is None else message.model_dump_json(by_alias=True, exclude_none=True))
    return out


@pytest.fixture
async def sqlite_factory():
    conns: list = []

    async def make():
        conn = await aiosqlite.connect(":memory:")
        conns.append(conn)
        store = SQLiteEventStore(conn, table_name="ev", ttl=None)
        await store.initialize()
        return store

    try:
        yield make
    finally:
        for conn in conns:
            await conn.close()


@pytest.fixture
async def redis_store():
    client = fakeredis.FakeRedis()
    store = RedisEventStore(client, key_prefix="mig:", ttl=None)
    try:
        yield store
    finally:
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


@pytest.mark.anyio
async def test_migrate_all_streams_preserves_order_and_payloads(sqlite_factory):
    source = await sqlite_factory()
    dest = await sqlite_factory()

    for i in range(3):
        await source.store_event("stream-A", _msg(i))
    for i in range(2):
        await source.store_event("stream-B", _msg(100 + i))

    result = await migrate(source, dest)

    assert isinstance(result, MigrationResult)
    assert result.streams_migrated == 2
    assert result.events_migrated == 5
    assert result.failed_streams == []

    assert await _dump(dest, "stream-A") == await _dump(source, "stream-A")
    assert await _dump(dest, "stream-B") == await _dump(source, "stream-B")


@pytest.mark.anyio
async def test_migrate_preserves_priming_events(sqlite_factory):
    source = await sqlite_factory()
    dest = await sqlite_factory()

    await source.store_event("stream-A", None)  # priming
    await source.store_event("stream-A", _msg(1))
    await source.store_event("stream-A", None)  # priming
    await source.store_event("stream-A", _msg(2))

    result = await migrate(source, dest)

    assert result.events_migrated == 4
    dumped = await _dump(dest, "stream-A")
    assert dumped[0] is None
    assert dumped[2] is None
    assert dumped == await _dump(source, "stream-A")


@pytest.mark.anyio
async def test_migrate_single_stream_scoping(sqlite_factory):
    source = await sqlite_factory()
    dest = await sqlite_factory()

    await source.store_event("stream-A", _msg(1))
    await source.store_event("stream-B", _msg(2))

    result = await migrate(source, dest, stream_id="stream-A")

    assert result.streams_migrated == 1
    assert result.events_migrated == 1
    assert await _dump(dest, "stream-A") == await _dump(source, "stream-A")
    # stream-B was not migrated.
    assert await _dump(dest, "stream-B") == []


@pytest.mark.anyio
async def test_migrate_on_progress_fires_per_batch_and_at_end(sqlite_factory):
    source = await sqlite_factory()
    dest = await sqlite_factory()

    for i in range(5):
        await source.store_event("stream-A", _msg(i))

    calls: list[tuple[str, int]] = []
    result = await migrate(
        source,
        dest,
        batch_size=2,
        on_progress=lambda sid, n: calls.append((sid, n)),
    )

    assert result.events_migrated == 5
    # Fires at 2 and 4 (every batch_size), then a final tick at 5.
    assert calls == [("stream-A", 2), ("stream-A", 4), ("stream-A", 5)]


@pytest.mark.anyio
async def test_migrate_continues_after_one_stream_fails(sqlite_factory, monkeypatch):
    source = await sqlite_factory()
    dest = await sqlite_factory()

    await source.store_event("stream-A", _msg(1))
    await source.store_event("stream-B", _msg(2))
    await source.store_event("stream-C", _msg(3))

    real_store_event = dest.store_event

    async def flaky(stream_id, message):
        if stream_id == "stream-B":
            raise RuntimeError("destination rejected stream-B")
        return await real_store_event(stream_id, message)

    monkeypatch.setattr(dest, "store_event", flaky)

    result = await migrate(source, dest)

    assert result.failed_streams == ["stream-B"]
    assert result.streams_migrated == 2
    assert result.events_migrated == 2
    # The healthy streams still made it across.
    assert await _dump(dest, "stream-A") == await _dump(source, "stream-A")
    assert await _dump(dest, "stream-C") == await _dump(source, "stream-C")


@pytest.mark.anyio
async def test_migrate_rejects_non_positive_batch_size(sqlite_factory):
    source = await sqlite_factory()
    dest = await sqlite_factory()
    with pytest.raises(ValueError, match="batch_size"):
        await migrate(source, dest, batch_size=0)


@pytest.mark.anyio
async def test_migrate_sqlite_to_redis(sqlite_factory, redis_store):
    source = await sqlite_factory()

    await source.store_event("stream-A", None)
    for i in range(3):
        await source.store_event("stream-A", _msg(i))

    result = await migrate(source, redis_store)

    assert result.streams_migrated == 1
    assert result.events_migrated == 4
    assert await _dump(redis_store, "stream-A") == await _dump(source, "stream-A")


@pytest.mark.anyio
async def test_migrate_redis_to_sqlite(sqlite_factory, redis_store):
    dest = await sqlite_factory()

    for i in range(3):
        await redis_store.store_event("stream-A", _msg(i))
    await redis_store.store_event("stream-B", _msg(99))

    result = await migrate(redis_store, dest)

    assert result.streams_migrated == 2
    assert result.events_migrated == 4
    assert await _dump(dest, "stream-A") == await _dump(redis_store, "stream-A")
    assert await _dump(dest, "stream-B") == await _dump(redis_store, "stream-B")
