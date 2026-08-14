# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""The record store behaves the same on every backend.

Records are the cross-version half of this library: the event store is only
consulted on the handshake-era transport, so this surface is what keeps working
when a client negotiates 2026-07-28. Running one contract against all three
backends is what stopped the session registry from drifting, so records get the
same treatment.

Redis uses fakeredis by default (a real server via MCP_TEST_REDIS_URL) and
Postgres is skipped unless MCP_TEST_POSTGRES_URL is set, which is how CI runs it.
"""

from __future__ import annotations

import os
import time
from typing import Any

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest

from mcp_persist import PostgresEventStore, RedisEventStore, SQLiteEventStore
from mcp_persist.records import Record, record_store_for

REAL_REDIS_URL = os.environ.get("MCP_TEST_REDIS_URL")
POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL")

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(params=["sqlite", "redis", "postgres"])
async def records(request):
    """One open record store per backend, so every test runs the same contract."""
    backend = request.param

    if backend == "sqlite":
        conn = await aiosqlite.connect(":memory:")
        store = SQLiteEventStore(conn, table_name="events", ttl=None)
        await store.initialize()
        records = record_store_for(store)
        await records.initialize()
        try:
            yield records
        finally:
            await conn.close()

    elif backend == "redis":
        if REAL_REDIS_URL:
            import redis.asyncio as real_redis

            client = real_redis.from_url(REAL_REDIS_URL)
            await client.flushdb()
        else:
            client = fakeredis.FakeRedis()
        records = record_store_for(RedisEventStore(client, key_prefix="recordtest:"))
        await records.initialize()
        try:
            yield records
        finally:
            if REAL_REDIS_URL:
                await client.flushdb()
            await client.aclose()

    else:
        if not POSTGRES_URL:
            pytest.skip("set MCP_TEST_POSTGRES_URL to run the Postgres record store tests")
        async with PostgresEventStore.create(POSTGRES_URL, table_name="mcp_events_recordtest") as store:
            records = record_store_for(store, table_name="mcp_records_test")
            await records.initialize()
            # Leave no rows behind from an interrupted earlier run.
            await records.purge(older_than=0)
            try:
                yield records
            finally:
                await records.purge(older_than=0)


def _record(**kwargs: Any) -> Record:
    base: dict[str, Any] = {"protocol_version": "2026-07-28", "method": "tools/call"}
    base.update(kwargs)
    return Record(**base)


async def test_store_then_list_roundtrips(records) -> None:
    written = _record(
        outcome="ok",
        carrier="middleware",
        duration_ms=12.5,
        request_id="7",
        tool_name="search",
    )

    await records.store_record(written)
    listed = await records.list_records()

    assert len(listed) == 1
    got = listed[0]
    assert got.record_id == written.record_id
    assert got.protocol_version == "2026-07-28"
    assert got.method == "tools/call"
    assert got.outcome == "ok"
    assert got.carrier == "middleware"
    assert got.duration_ms == pytest.approx(12.5)
    assert got.request_id == "7"
    assert got.tool_name == "search"
    assert got.payload is None
    assert got.payload_truncated is False


async def test_payload_roundtrips_through_the_stores_codec(records) -> None:
    await records.store_record(_record(payload={"query": "kittens"}, payload_truncated=True))

    got = (await records.list_records())[0]

    assert got.payload == {"query": "kittens"}
    assert got.payload_truncated is True


async def test_records_are_listed_newest_first(records) -> None:
    now = time.time()
    await records.store_records(
        [
            _record(method="old", recorded_at=now - 30),
            _record(method="new", recorded_at=now),
            _record(method="middle", recorded_at=now - 15),
        ]
    )

    listed = await records.list_records()

    assert [r.method for r in listed] == ["new", "middle", "old"]


async def test_limit_is_honoured(records) -> None:
    now = time.time()
    await records.store_records([_record(recorded_at=now - i) for i in range(5)])

    assert len(await records.list_records(limit=2)) == 2


async def test_store_records_returns_the_count(records) -> None:
    assert await records.store_records([]) == 0
    assert await records.store_records([_record(), _record()]) == 2


async def test_filter_by_method(records) -> None:
    await records.store_records([_record(method="tools/call"), _record(method="prompts/get")])

    listed = await records.list_records(method="prompts/get")

    assert [r.method for r in listed] == ["prompts/get"]


async def test_filter_by_outcome(records) -> None:
    await records.store_records([_record(outcome="ok"), _record(outcome="cancelled")])

    listed = await records.list_records(outcome="cancelled")

    assert [r.outcome for r in listed] == ["cancelled"]


async def test_filter_by_since(records) -> None:
    now = time.time()
    await records.store_records(
        [_record(method="ancient", recorded_at=now - 600), _record(method="recent", recorded_at=now)]
    )

    listed = await records.list_records(since=now - 60)

    assert [r.method for r in listed] == ["recent"]


async def test_count_reflects_stored_records(records) -> None:
    assert await records.count() == 0

    await records.store_records([_record(), _record(), _record()])

    assert await records.count() == 3


async def test_purge_deletes_only_old_records(records) -> None:
    now = time.time()
    await records.store_records(
        [_record(method="ancient", recorded_at=now - 600), _record(method="recent", recorded_at=now)]
    )

    deleted = await records.purge(older_than=60)

    assert deleted == 1
    assert [r.method for r in await records.list_records()] == ["recent"]


async def test_purge_on_an_empty_store_is_zero(records) -> None:
    assert await records.purge(older_than=0) == 0


async def test_error_outcomes_carry_a_code_and_no_text(records) -> None:
    """A failure is a fixed outcome plus a numeric code; there is nowhere to put text."""
    await records.store_record(_record(outcome="mcp_error", error_code=-32602))

    got = (await records.list_records())[0]

    assert got.outcome == "mcp_error"
    assert got.error_code == -32602
    assert not hasattr(got, "error_message")


async def test_notifications_have_no_request_id(records) -> None:
    await records.store_record(_record(method="notifications/cancelled", kind="notification"))

    got = (await records.list_records())[0]

    assert got.kind == "notification"
    assert got.request_id is None


async def test_era_is_recorded_per_record(records) -> None:
    """One store spans a migration, so each record carries its own era."""
    await records.store_records(
        [
            _record(protocol_version="2025-11-25", method="legacy"),
            _record(protocol_version="2026-07-28", method="modern"),
        ]
    )

    listed = await records.list_records()

    assert {r.method: r.protocol_version for r in listed} == {
        "legacy": "2025-11-25",
        "modern": "2026-07-28",
    }


async def test_redis_index_does_not_grow_past_the_ttl() -> None:
    """A record hash expires on its own; its index member must go too.

    Otherwise `count()` climbs forever and reports records Redis has already
    reclaimed. Redis-only: it is the one backend with native expiry.
    """
    client = fakeredis.FakeRedis()
    try:
        records = record_store_for(RedisEventStore(client, key_prefix="ttltest:"), ttl=60)
        await records.initialize()
        now = time.time()

        # Two records older than the ttl, one inside it.
        await records.store_records(
            [
                _record(method="ancient", recorded_at=now - 600),
                _record(method="old", recorded_at=now - 300),
            ]
        )
        await records.store_record(_record(method="fresh"))

        assert await records.count() == 1
        assert [r.method for r in await records.list_records()] == ["fresh"]
    finally:
        await client.aclose()


async def test_redis_ttl_uses_the_records_own_age() -> None:
    """Hash expiry and index score must agree, or count() drifts from reality.

    A record delayed in the writer queue, or backfilled with an older
    `recorded_at`, would otherwise be evicted from the index while its hash was
    still alive, making count() report fewer records than exist.
    """
    client = fakeredis.FakeRedis()
    try:
        records = record_store_for(RedisEventStore(client, key_prefix="agetest:"), ttl=300)
        await records.initialize()
        now = time.time()

        # Backdated but still inside the ttl: must be stored and counted.
        assert await records.store_records([_record(method="delayed", recorded_at=now - 200)]) == 1
        assert await records.count() == 1
        assert [r.method for r in await records.list_records()] == ["delayed"]

        # Older than the ttl: not stored at all, rather than stored and hidden.
        assert await records.store_records([_record(method="expired", recorded_at=now - 400)]) == 0
        assert await records.count() == 1
    finally:
        await client.aclose()


async def test_duplicate_ids_are_not_reported_as_written(records) -> None:
    """The writer trusts this count, so it must reflect rows that really landed."""
    record = _record(method="once")

    assert await records.store_records([record]) == 1
    second = await records.store_records([record])

    # SQLite upserts (1); Postgres and Redis dedupe or overwrite. Either way the
    # store must not claim to have written more rows than the batch held.
    assert second <= 1
    assert await records.count() == 1
