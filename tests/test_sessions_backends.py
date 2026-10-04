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
from typing import Any

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
            try:
                await client.aclose()
            except AttributeError:  # redis-py < 5.0
                await client.close(close_connection_pool=True)

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
    # ...and must not leave a second, live copy behind in the listing.
    assert [r.session_id for r in await registry.list_sessions()] == []
    assert [r.session_id for r in await registry.list_sessions(include_terminated=True)] == ["sess-a"]


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


async def test_sqlite_rows_from_before_the_tenant_key_fix_are_merged() -> None:
    # Before 2.1.1 an unbound SQLite registry stored tenant_id as NULL, which
    # SQLite treats as distinct in a PRIMARY KEY, so one id could hold several
    # rows. Opening the table folds them into one, and an ended copy stays ended.
    conn = await aiosqlite.connect(":memory:")
    try:
        await conn.execute(
            "CREATE TABLE mcp_sessions (session_id TEXT NOT NULL, tenant_id TEXT, created_at REAL NOT NULL, "
            "last_seen_at REAL NOT NULL, terminated INTEGER NOT NULL DEFAULT 0, owner TEXT, "
            "PRIMARY KEY (session_id, tenant_id))"
        )
        await conn.executemany(
            "INSERT INTO mcp_sessions VALUES (?, NULL, ?, ?, ?, NULL)",
            [("dup", 10.0, 20.0, 1), ("dup", 30.0, 40.0, 0), ("solo", 5.0, 6.0, 0)],
        )
        store = SQLiteEventStore(conn, table_name="events", ttl=None)
        await store.initialize()
        registry = session_registry_for(store)
        await registry.initialize()

        dup = await registry.get("dup")
        assert dup is not None
        assert (dup.created_at, dup.last_seen_at, dup.terminated) == (10.0, 40.0, True)
        assert [r.session_id for r in await registry.list_sessions()] == ["solo"]
        async with conn.execute("SELECT COUNT(*) FROM mcp_sessions WHERE tenant_id IS NULL") as cursor:
            assert (await cursor.fetchone())[0] == 0
    finally:
        await conn.close()


async def test_handshake_round_trips_and_the_first_one_is_kept(registry) -> None:
    handshake = {"protocolVersion": "2025-06-18", "capabilities": {"sampling": {}}, "clientInfo": {"name": "c"}}
    await registry.register("sess-h", handshake=handshake)
    record = await registry.get("sess-h")
    assert record is not None and record.handshake == handshake

    # A re-register never replaces what the session was actually opened with.
    await registry.register("sess-h", handshake={"protocolVersion": "other"})
    again = await registry.get("sess-h")
    assert again is not None and again.handshake == handshake

    await registry.register("sess-none")
    none = await registry.get("sess-none")
    assert none is not None and none.handshake is None
    assert [r.handshake for r in await registry.list_sessions() if r.session_id == "sess-h"] == [handshake]


async def test_re_register_never_rebinds_the_owner(registry) -> None:
    owner = {"client_id": "a", "issuer": "https://idp", "subject": "alice"}
    await registry.register("sess-o", owner=owner)

    # Adoption compares the requesting principal against this record, so a
    # re-register naming someone else must not become a way to take the session.
    await registry.register("sess-o", owner={"client_id": "b", "issuer": "https://idp", "subject": "mallory"})
    record = await registry.get("sess-o")
    assert record is not None and record.owner == owner

    await registry.register("sess-anon")
    await registry.register("sess-anon", owner=owner)
    anon = await registry.get("sess-anon")
    assert anon is not None and anon.owner is None


class _EndsBeforeTheTransaction:
    """Wraps a pipeline so the racing `terminate` lands as it is opened."""

    def __init__(self, pipe, racer):
        self._pipe = pipe
        self._racer = racer

    async def __aenter__(self):
        await self._racer.fire()
        return await self._pipe.__aenter__()

    async def __aexit__(self, *exc):
        return await self._pipe.__aexit__(*exc)


async def test_redis_register_cannot_revive_a_session_ended_mid_register() -> None:
    client = fakeredis.FakeRedis()
    try:
        registry = session_registry_for(RedisEventStore(client, key_prefix="racetest:"))
        await registry.register("sess-r")

        class EndsTheSessionAfterTheFirstCommand:
            """Lets one `terminate` land inside register, between its read and its write.

            After the first plain command (the read a read-then-write register
            starts with), or else just before it opens a transaction.
            """

            def __init__(self, inner):
                self._inner = inner
                self._fired = False

            async def fire(self):
                if not self._fired:
                    self._fired = True
                    await registry.terminate("sess-r")

            def __getattr__(self, name):
                attr = getattr(self._inner, name)
                if not callable(attr):
                    return attr

                if name == "pipeline":

                    def open_pipeline(*args, **kwargs):
                        return _EndsBeforeTheTransaction(attr(*args, **kwargs), self)

                    return open_pipeline

                async def call(*args, **kwargs):
                    result = await attr(*args, **kwargs)
                    await self.fire()
                    return result

                return call

        real = registry._redis
        registry._redis = EndsTheSessionAfterTheFirstCommand(real)
        await registry.register("sess-r")
        registry._redis = real

        record = await registry.get("sess-r")
        assert record is not None and record.terminated
    finally:
        try:
            await client.aclose()
        except AttributeError:  # redis-py < 5.0
            await client.close(close_connection_pool=True)


async def test_sqlite_table_from_before_the_handshake_column_is_upgraded() -> None:
    conn = await aiosqlite.connect(":memory:")
    try:
        await conn.execute(
            "CREATE TABLE mcp_sessions (session_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '', "
            "created_at REAL NOT NULL, last_seen_at REAL NOT NULL, terminated INTEGER NOT NULL DEFAULT 0, "
            "owner TEXT, PRIMARY KEY (session_id, tenant_id))"
        )
        await conn.execute("INSERT INTO mcp_sessions VALUES ('old', '', 1.0, 2.0, 0, NULL)")
        store = SQLiteEventStore(conn, table_name="events", ttl=None)
        await store.initialize()
        registry = session_registry_for(store)
        await registry.initialize()

        old = await registry.get("old")
        assert old is not None and old.handshake is None
        await registry.register("new", handshake={"protocolVersion": "2025-06-18"})
        new = await registry.get("new")
        assert new is not None and new.handshake == {"protocolVersion": "2025-06-18"}
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_redis_a_partial_session_hash_is_not_a_session() -> None:
    # `touch` checks the key exists and then writes last_seen_at in a second round
    # trip. If the session's ttl expires in between, that write recreates the hash
    # with only last_seen_at in it: no created_at, no owner, terminated unset. Read
    # back as a record, that was an expired session alive again, adoptable by any
    # unauthenticated caller. `register` always writes created_at, so a hash
    # without it was never registered and is not a session.
    client = fakeredis.FakeRedis()
    try:
        registry = session_registry_for(RedisEventStore(client, key_prefix="partial:"))
        await client.hset("partial:session:sess-expired", "last_seen_at", "123.0")
        await client.zadd("partial:sessions", {"sess-expired": 123.0})

        assert await registry.get("sess-expired") is None
        assert [r.session_id for r in await registry.list_sessions(include_terminated=True)] == []
    finally:
        try:
            await client.aclose()
        except AttributeError:  # redis-py < 5.0
            await client.close(close_connection_pool=True)


@pytest.mark.anyio
async def test_redis_registry_transactions_stay_within_one_cluster_slot() -> None:
    # On Redis Cluster every key in a MULTI/EXEC must hash to one slot. register
    # once wrapped the session hash and the session index (a different slot) in
    # one transaction, so it raised CrossSlotTransactionError on every call there,
    # and durable sessions were never recorded. This client refuses the same thing.
    from redis.crc import key_slot

    real = fakeredis.FakeRedis()

    class _SlotCheckingPipeline:
        def __init__(self, inner, transaction):
            self._inner = inner
            self._transaction = transaction
            self._slots: set[int] = set()

        async def __aenter__(self):
            await self._inner.__aenter__()
            return self

        async def __aexit__(self, *exc):
            return await self._inner.__aexit__(*exc)

        def __getattr__(self, name):
            command = getattr(self._inner, name)
            if name == "execute" or not callable(command):
                return command

            def queue(key, *args, **kwargs):
                self._slots.add(key_slot(key.encode() if isinstance(key, str) else key))
                command(key, *args, **kwargs)
                return self

            return queue

        async def execute(self):
            if self._transaction and len(self._slots) > 1:
                raise AssertionError(f"transaction spans {len(self._slots)} cluster slots")
            return await self._inner.execute()

    class _ClusterRules:
        def __getattr__(self, name):
            return getattr(real, name)

        def pipeline(self, transaction=True, **kwargs):
            return _SlotCheckingPipeline(real.pipeline(transaction=transaction, **kwargs), transaction)

    try:
        registry = session_registry_for(RedisEventStore(_ClusterRules(), key_prefix="slots:"), ttl=60)
        await registry.register("sess-c", owner={"client_id": "a"}, handshake={"protocolVersion": "x"})
        await registry.touch("sess-c")
        await registry.terminate("sess-c")
        record = await registry.get("sess-c")
        assert record is not None and record.terminated and record.owner == {"client_id": "a"}
        assert [r.session_id for r in await registry.list_sessions(include_terminated=True)] == ["sess-c"]
    finally:
        try:
            await real.aclose()
        except AttributeError:  # redis-py < 5.0
            await real.close(close_connection_pool=True)


@pytest.mark.anyio
async def test_postgres_registry_starts_as_a_role_that_does_not_own_the_table() -> None:
    # initialize() ran ALTER TABLE ... ADD COLUMN IF NOT EXISTS every time. That
    # needs the table's owner even when the column exists, so an application role
    # with only read/write grants (the usual least-privilege setup) failed to start.
    if not POSTGRES_URL:
        pytest.skip("set MCP_TEST_POSTGRES_URL to run the Postgres session registry tests")
    import uuid

    import asyncpg

    from mcp_persist.sessions import PostgresSessionRegistry

    role = f"app_{uuid.uuid4().hex[:8]}"
    table = f"sessions_{uuid.uuid4().hex[:8]}"

    class _Store:
        def __init__(self, pool: Any) -> None:
            self._pool = pool
            self._tenant_id = None

    admin = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=2)
    try:
        await PostgresSessionRegistry(_Store(admin), table_name=table).initialize()  # created by the owner
        await admin.execute(f'CREATE ROLE "{role}" LOGIN')
        await admin.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON "{table}" TO "{role}"')
        await admin.execute(f'GRANT USAGE, CREATE ON SCHEMA public TO "{role}"')

        app = await asyncpg.create_pool(POSTGRES_URL, user=role, min_size=1, max_size=2)
        try:
            registry = PostgresSessionRegistry(_Store(app), table_name=table)
            await registry.initialize()
            await registry.register("sess-app", handshake={"protocolVersion": "x"})
            record = await registry.get("sess-app")
            assert record is not None and record.handshake == {"protocolVersion": "x"}
        finally:
            await app.close()
    finally:
        await admin.execute(f'DROP TABLE IF EXISTS "{table}"')
        await admin.execute(f'REVOKE ALL ON SCHEMA public FROM "{role}"')
        await admin.execute(f'DROP ROLE IF EXISTS "{role}"')
        await admin.close()


@pytest.mark.anyio
@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
async def test_a_registry_table_from_an_earlier_release_gains_the_handshake_column(backend: str) -> None:
    # A table created before handshakes were recorded has no handshake column;
    # initialize() adds it, and only then.
    import uuid

    from mcp_persist.sessions import PostgresSessionRegistry, SQLiteSessionRegistry

    table = f"old_sessions_{uuid.uuid4().hex[:8]}"
    if backend == "sqlite":
        conn = await aiosqlite.connect(":memory:")
        try:
            await conn.execute(
                f"CREATE TABLE {table} (session_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '', "
                "created_at REAL NOT NULL, last_seen_at REAL NOT NULL, terminated INTEGER NOT NULL DEFAULT 0, "
                "owner TEXT, PRIMARY KEY (session_id, tenant_id))"
            )
            store = SQLiteEventStore(conn, table_name="events", ttl=None)
            await store.initialize()
            registry: Any = SQLiteSessionRegistry(store, table_name=table)
            await registry.initialize()
            await registry.register("sess-old", handshake={"protocolVersion": "x"})
            record = await registry.get("sess-old")
        finally:
            await conn.close()
    else:
        if not POSTGRES_URL:
            pytest.skip("set MCP_TEST_POSTGRES_URL to run the Postgres session registry tests")
        import asyncpg

        class _Store:
            def __init__(self, pool: Any) -> None:
                self._pool = pool
                self._tenant_id = None

        pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=2)
        try:
            await pool.execute(
                f"CREATE TABLE \"{table}\" (session_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '', "
                "created_at DOUBLE PRECISION NOT NULL, last_seen_at DOUBLE PRECISION NOT NULL, "
                "terminated BOOLEAN NOT NULL DEFAULT FALSE, owner JSONB, PRIMARY KEY (session_id, tenant_id))"
            )
            registry = PostgresSessionRegistry(_Store(pool), table_name=table)
            await registry.initialize()
            await registry.register("sess-old", handshake={"protocolVersion": "x"})
            record = await registry.get("sess-old")
        finally:
            await pool.execute(f'DROP TABLE IF EXISTS "{table}"')
            await pool.close()

    assert record is not None and record.handshake == {"protocolVersion": "x"}


@pytest.mark.anyio
async def test_redis_a_hash_rebuilt_without_its_owner_is_not_a_session() -> None:
    # register writes its fields in a plain pipeline. If an existing session's key
    # expires partway through a re-register, the rest of the pipeline rebuilds a
    # hash with created_at and last_seen_at but no owner and no terminated flag,
    # which would otherwise read back as a live, ownerless session.
    client = fakeredis.FakeRedis()
    try:
        registry = session_registry_for(RedisEventStore(client, key_prefix="rebuilt:"))
        await client.hset("rebuilt:session:sess-x", mapping={"created_at": "1.0", "last_seen_at": "2.0"})
        await client.zadd("rebuilt:sessions", {"sess-x": 2.0})

        assert await registry.get("sess-x") is None
        assert await registry.list_sessions(include_terminated=True) == []

        # A complete record with no owner (an unauthenticated session) still reads back.
        await registry.register("sess-anon")
        record = await registry.get("sess-anon")
        assert record is not None and record.owner is None and not record.terminated
    finally:
        try:
            await client.aclose()
        except AttributeError:  # redis-py < 5.0
            await client.close(close_connection_pool=True)
