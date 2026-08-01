# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""The session registry behaves the same on every backend.

SQLite is covered end-to-end in test_durable_sessions.py; this module runs the
same contract against Redis (fakeredis by default, a real server via
MCP_TEST_REDIS_URL) and Postgres (skipped unless MCP_TEST_POSTGRES_URL is set,
which is how CI runs it), so a backend cannot drift from the others.
"""

from __future__ import annotations

import os

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest

from mcp_persist import PostgresEventStore, RedisEventStore, SQLiteEventStore
from mcp_persist.sessions import session_registry_for

REAL_REDIS_URL = os.environ.get("MCP_TEST_REDIS_URL")
POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL")

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(params=["sqlite", "redis", "postgres"])
async def registry(request):
    """One open registry per backend, so every test runs the same contract."""
    backend = request.param

    if backend == "sqlite":
        conn = await aiosqlite.connect(":memory:")
        store = SQLiteEventStore(conn, table_name="events", ttl=None)
        await store.initialize()
        registry = session_registry_for(store)
        await registry.initialize()
        try:
            yield registry
        finally:
            await conn.close()

    elif backend == "redis":
        if REAL_REDIS_URL:
            import redis.asyncio as real_redis

            client = real_redis.from_url(REAL_REDIS_URL)
            await client.flushdb()
        else:
            client = fakeredis.FakeRedis()
        registry = session_registry_for(RedisEventStore(client, key_prefix="sessiontest:"))
        await registry.initialize()
        try:
            yield registry
        finally:
            if REAL_REDIS_URL:
                await client.flushdb()
            await client.aclose()

    else:
        if not POSTGRES_URL:
            pytest.skip("set MCP_TEST_POSTGRES_URL to run the Postgres session registry tests")
        async with PostgresEventStore.create(POSTGRES_URL, table_name="mcp_events_sessiontest") as store:
            registry = session_registry_for(store, table_name="mcp_sessions_test")
            await registry.initialize()
            # Leave no rows behind from an interrupted earlier run.
            await registry.purge(older_than=0)
            try:
                yield registry
            finally:
                await registry.purge(older_than=0)


async def test_register_then_get_roundtrips(registry) -> None:
    owner = {"client_id": "cli", "issuer": "https://idp", "subject": "user-1"}
    await registry.register("sess-a", owner=owner)

    record = await registry.get("sess-a")
    assert record is not None
    assert record.session_id == "sess-a"
    assert record.terminated is False
    assert record.owner == owner
    assert record.created_at > 0
    assert record.last_seen_at >= record.created_at


async def test_unknown_session_is_none(registry) -> None:
    assert await registry.get("never-existed") is None


async def test_touch_moves_last_seen_forward(registry) -> None:
    await registry.register("sess-a")
    before = await registry.get("sess-a")
    assert before is not None

    await registry.touch("sess-a")
    after = await registry.get("sess-a")
    assert after is not None
    assert after.last_seen_at >= before.last_seen_at
    # Touching must not disturb the creation time or resurrect anything.
    assert after.created_at == pytest.approx(before.created_at)


async def test_touch_of_unknown_session_is_a_no_op(registry) -> None:
    await registry.touch("never-existed")
    assert await registry.get("never-existed") is None


async def test_terminate_is_visible_and_sticky(registry) -> None:
    await registry.register("sess-a")
    await registry.terminate("sess-a")

    record = await registry.get("sess-a")
    assert record is not None and record.terminated is True

    # Re-registering the same id must not clear the flag: that would let a
    # terminated session be adopted again.
    await registry.register("sess-a")
    again = await registry.get("sess-a")
    assert again is not None and again.terminated is True


async def test_list_orders_by_recency_and_filters_terminated(registry) -> None:
    await registry.register("older")
    await registry.register("newer")
    await registry.touch("newer")

    live = await registry.list_sessions()
    assert [r.session_id for r in live][:2] == ["newer", "older"]

    await registry.terminate("older")
    assert [r.session_id for r in await registry.list_sessions()] == ["newer"]

    everything = {r.session_id for r in await registry.list_sessions(include_terminated=True)}
    assert everything == {"newer", "older"}


async def test_list_respects_limit(registry) -> None:
    for i in range(5):
        await registry.register(f"sess-{i}")
    assert len(await registry.list_sessions(limit=2)) == 2


async def test_purge_deletes_by_age(registry) -> None:
    await registry.register("sess-a")
    # Nothing is an hour old yet.
    assert await registry.purge(older_than=3600) == 0
    assert await registry.get("sess-a") is not None
    # A zero-second window makes everything stale.
    assert await registry.purge(older_than=0) == 1
    assert await registry.get("sess-a") is None
    assert await registry.list_sessions(include_terminated=True) == []


async def test_registry_rejects_a_backend_without_one() -> None:
    class NotAStore:
        pass

    with pytest.raises(TypeError, match="no session registry"):
        session_registry_for(NotAStore())  # type: ignore[arg-type]
