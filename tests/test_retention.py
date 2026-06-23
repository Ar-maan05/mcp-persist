# pyright: reportPrivateUsage=false
# pyright: reportArgumentType=false
# pyright: reportAttributeAccessIssue=false
"""Tests for retention policy and scheduler components.

Covers RetentionPolicy, DeletionAuditEntry, RetentionScheduler, the audit sinks,
and the config parsers.
"""

from __future__ import annotations

import asyncio
import json
import os
import time

import aiosqlite
import pytest
from mcp.types import JSONRPCRequest

from mcp_persist import (
    DatabaseAuditSink,
    DeletionAuditEntry,
    LoggingAuditSink,
    NoOpAuditSink,
    RetentionPolicy,
    RetentionScheduler,
    SQLiteEventStore,
    retention_policy_from_env,
)

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")
POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL")


# ── Policy Tests ─────────────────────────────────────────────────────────────


def test_policy_window_for():
    # Setup windows and default
    policy = RetentionPolicy(windows={"team-a": 3600, "team-b": 7200, None: 600}, default=300)
    assert policy.window_for("team-a") == 3600
    assert policy.window_for("team-b") == 7200
    assert policy.window_for(None) == 600
    assert policy.window_for("unknown-team") == 300

    # Policy with no default and no None key
    policy_no_def = RetentionPolicy(windows={"team-a": 3600})
    assert policy_no_def.window_for("unknown") is None
    assert policy_no_def.window_for(None) is None


def test_policy_validation():
    # Negative window
    with pytest.raises(ValueError, match="All window values must be integers greater than 0"):
        RetentionPolicy(windows={"team-a": -1})

    # Zero window
    with pytest.raises(ValueError, match="All window values must be integers greater than 0"):
        RetentionPolicy(windows={"team-a": 0})

    # Non-integer window
    with pytest.raises(ValueError, match="All window values must be integers greater than 0"):
        RetentionPolicy(windows={"team-a": "3600"})  # type: ignore

    # Negative default
    with pytest.raises(ValueError, match="default window must be an integer greater than 0"):
        RetentionPolicy(windows={}, default=-5)

    # Zero default
    with pytest.raises(ValueError, match="default window must be an integer greater than 0"):
        RetentionPolicy(windows={}, default=0)

    # Non-integer default
    with pytest.raises(ValueError, match="default window must be an integer greater than 0"):
        RetentionPolicy(windows={}, default="300")  # type: ignore


def test_policy_clones_windows():
    mutable_windows = {"team-a": 3600}
    policy = RetentionPolicy(windows=mutable_windows)

    # Mutating original dict should not affect policy behavior
    mutable_windows["team-a"] = 7200
    assert policy.window_for("team-a") == 3600


# ── SQLite Event Store Tests ──────────────────────────────────────────────────


@pytest.fixture
async def sqlite_conn():
    conn = await aiosqlite.connect(":memory:")
    try:
        yield conn
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_sqlite_purge_tenant_and_distinct(sqlite_conn):
    store = SQLiteEventStore(sqlite_conn, table_name="test_events", ttl=None)
    await store.initialize()

    # Store events with explicit tenants
    store_a = SQLiteEventStore(sqlite_conn, table_name="test_events", tenant_id="team-a")
    store_b = SQLiteEventStore(sqlite_conn, table_name="test_events", tenant_id="team-b")
    store_none = SQLiteEventStore(sqlite_conn, table_name="test_events", tenant_id=None)

    # Insert events
    await store_a.store_event("s1", SAMPLE_MSG)  # team-a event
    await store_b.store_event("s2", SAMPLE_MSG)  # team-b event
    await store_none.store_event("s3", SAMPLE_MSG)  # untenanted event

    # Check distinct tenants
    tenants = await store.distinct_tenants()
    assert set(tenants) == {"team-a", "team-b", None}

    # Manually age events in DB
    now = time.time()
    # team-a event aged by 100 seconds
    await sqlite_conn.execute("UPDATE test_events SET created_at = ? WHERE tenant_id = 'team-a'", (now - 100,))
    # team-b event aged by 20 seconds
    await sqlite_conn.execute("UPDATE test_events SET created_at = ? WHERE tenant_id = 'team-b'", (now - 20,))
    # untenanted event aged by 100 seconds
    await sqlite_conn.execute("UPDATE test_events SET created_at = ? WHERE tenant_id IS NULL", (now - 100,))
    await sqlite_conn.commit()

    # Purge team-a with 50s window (should delete team-a)
    count = await store.purge_tenant("team-a", window_seconds=50)
    assert count == 1

    # team-a should be gone, but team-b and untenanted should remain
    async with sqlite_conn.execute("SELECT COUNT(*) FROM test_events WHERE tenant_id = 'team-a'") as cur:
        assert (await cur.fetchone())[0] == 0
    async with sqlite_conn.execute("SELECT COUNT(*) FROM test_events WHERE tenant_id = 'team-b'") as cur:
        assert (await cur.fetchone())[0] == 1
    async with sqlite_conn.execute("SELECT COUNT(*) FROM test_events WHERE tenant_id IS NULL") as cur:
        assert (await cur.fetchone())[0] == 1

    # Purge untenanted with 50s window (should delete untenanted)
    count = await store.purge_tenant(None, window_seconds=50)
    assert count == 1
    async with sqlite_conn.execute("SELECT COUNT(*) FROM test_events WHERE tenant_id IS NULL") as cur:
        assert (await cur.fetchone())[0] == 0

    # team-b should still be there because its age (20s) is less than window (50s)
    count = await store.purge_tenant("team-b", window_seconds=50)
    assert count == 0
    async with sqlite_conn.execute("SELECT COUNT(*) FROM test_events WHERE tenant_id = 'team-b'") as cur:
        assert (await cur.fetchone())[0] == 1


@pytest.mark.anyio
async def test_sqlite_purge_tenant_batching(sqlite_conn):
    store = SQLiteEventStore(sqlite_conn, table_name="test_events_batch", ttl=None)
    await store.initialize()

    store_a = SQLiteEventStore(sqlite_conn, table_name="test_events_batch", tenant_id="team-a")
    for _ in range(5):
        await store_a.store_event("s", SAMPLE_MSG)

    # Age them all
    await sqlite_conn.execute("UPDATE test_events_batch SET created_at = ?", (time.time() - 100,))
    await sqlite_conn.commit()

    # Purge with batch_size = 2
    count = await store.purge_tenant("team-a", window_seconds=50, batch_size=2)
    assert count == 5

    async with sqlite_conn.execute("SELECT COUNT(*) FROM test_events_batch") as cur:
        assert (await cur.fetchone())[0] == 0


# ── Postgres Event Store Tests ────────────────────────────────────────────────


@pytest.mark.skipif(POSTGRES_URL is None, reason="set MCP_TEST_POSTGRES_URL to run Postgres tests")
@pytest.mark.anyio
async def test_postgres_purge_tenant_and_distinct():
    import asyncpg

    from mcp_persist import PostgresEventStore

    pool = await asyncpg.create_pool(POSTGRES_URL)
    assert pool is not None
    try:
        # Recreate table
        await pool.execute("DROP TABLE IF EXISTS test_retention_events")
        store = PostgresEventStore(pool, table_name="test_retention_events", ttl=None)
        await store.initialize()

        store_a = PostgresEventStore(pool, table_name="test_retention_events", tenant_id="team-a")
        store_b = PostgresEventStore(pool, table_name="test_retention_events", tenant_id="team-b")
        store_none = PostgresEventStore(pool, table_name="test_retention_events", tenant_id=None)

        await store_a.store_event("s1", SAMPLE_MSG)
        await store_b.store_event("s2", SAMPLE_MSG)
        await store_none.store_event("s3", SAMPLE_MSG)

        # distinct_tenants
        tenants = await store.distinct_tenants()
        assert set(tenants) == {"team-a", "team-b", None}

        # Age events
        now = time.time()
        await pool.execute("UPDATE test_retention_events SET created_at = $1 WHERE tenant_id = 'team-a'", now - 100)
        await pool.execute("UPDATE test_retention_events SET created_at = $1 WHERE tenant_id = 'team-b'", now - 20)
        await pool.execute("UPDATE test_retention_events SET created_at = $1 WHERE tenant_id IS NULL", now - 100)

        # Purge team-a
        count = await store.purge_tenant("team-a", window_seconds=50)
        assert count == 1
        assert (await pool.fetchval("SELECT COUNT(*) FROM test_retention_events WHERE tenant_id = 'team-a'")) == 0

        # Purge None (untenanted)
        count = await store.purge_tenant(None, window_seconds=50)
        assert count == 1
        assert (await pool.fetchval("SELECT COUNT(*) FROM test_retention_events WHERE tenant_id IS NULL")) == 0

        # Purge team-b with batching
        await store_b.store_event("s2", SAMPLE_MSG)
        await store_b.store_event("s2", SAMPLE_MSG)
        await pool.execute("UPDATE test_retention_events SET created_at = $1 WHERE tenant_id = 'team-b'", now - 100)

        count = await store.purge_tenant("team-b", window_seconds=50, batch_size=1)
        assert count == 3
        assert (await pool.fetchval("SELECT COUNT(*) FROM test_retention_events WHERE tenant_id = 'team-b'")) == 0
    finally:
        await pool.execute("DROP TABLE IF EXISTS test_retention_events")
        await pool.close()


# ── Audit Sinks Tests ─────────────────────────────────────────────────────────


class CapturingLogger:
    def __init__(self):
        self.lines = []

    def info(self, msg, *args):
        self.lines.append(msg)


@pytest.mark.anyio
async def test_noop_audit_sink():
    sink = NoOpAuditSink()
    entry = DeletionAuditEntry(
        timestamp=100.0,
        tenant_id="team-a",
        window_seconds=3600,
        cutoff=50.0,
        deleted_count=5,
        backend="sqlite",
        source_table="ev",
        default_applied=False,
    )
    await sink.record(entry)  # Should not raise or do anything


@pytest.mark.anyio
async def test_logging_audit_sink():
    logger = CapturingLogger()
    sink = LoggingAuditSink(logger=logger)  # type: ignore
    entry = DeletionAuditEntry(
        timestamp=100.0,
        tenant_id="team-a",
        window_seconds=3600,
        cutoff=50.0,
        deleted_count=5,
        backend="sqlite",
        source_table="ev",
        default_applied=False,
    )
    await sink.record(entry)
    assert len(logger.lines) == 1
    data = json.loads(logger.lines[0])
    assert data["tenant_id"] == "team-a"
    assert data["deleted_count"] == 5
    assert data["backend"] == "sqlite"


@pytest.mark.anyio
async def test_database_audit_sink_sqlite(sqlite_conn):
    store = SQLiteEventStore(sqlite_conn, table_name="test_ev", ttl=None)
    await store.initialize()

    sink = DatabaseAuditSink(store)
    entry = DeletionAuditEntry(
        timestamp=100.0,
        tenant_id="team-a",
        window_seconds=3600,
        cutoff=50.0,
        deleted_count=5,
        backend="sqlite",
        source_table='"test_ev"',
        default_applied=False,
    )

    await sink.record(entry)

    # Check table was created and has entry
    async with sqlite_conn.execute("SELECT * FROM test_ev_retention_audit") as cur:
        rows = await cur.fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert row[1] == 100.0
        assert row[2] == "team-a"
        assert row[3] == 3600
        assert row[4] == 50.0
        assert row[5] == 5
        assert row[6] == "sqlite"
        assert row[7] == '"test_ev"'
        assert row[8] == 0


@pytest.mark.skipif(POSTGRES_URL is None, reason="set MCP_TEST_POSTGRES_URL to run Postgres tests")
@pytest.mark.anyio
async def test_database_audit_sink_postgres():
    import asyncpg

    from mcp_persist import PostgresEventStore

    pool = await asyncpg.create_pool(POSTGRES_URL)
    assert pool is not None
    try:
        await pool.execute("DROP TABLE IF EXISTS test_ev")
        await pool.execute("DROP TABLE IF EXISTS test_ev_retention_audit")

        store = PostgresEventStore(pool, table_name="test_ev", ttl=None)
        await store.initialize()

        sink = DatabaseAuditSink(store)
        entry = DeletionAuditEntry(
            timestamp=100.0,
            tenant_id="team-a",
            window_seconds=3600,
            cutoff=50.0,
            deleted_count=5,
            backend="postgres",
            source_table='"test_ev"',
            default_applied=True,
        )

        await sink.record(entry)

        row = await pool.fetchrow("SELECT * FROM test_ev_retention_audit")
        assert row is not None
        assert row["ts"] == 100.0
        assert row["tenant_id"] == "team-a"
        assert row["window_seconds"] == 3600
        assert row["cutoff"] == 50.0
        assert row["deleted_count"] == 5
        assert row["backend"] == "postgres"
        assert row["source_table"] == '"test_ev"'
        assert row["default_applied"] is True
    finally:
        await pool.execute("DROP TABLE IF EXISTS test_ev")
        await pool.execute("DROP TABLE IF EXISTS test_ev_retention_audit")
        await pool.close()


# ── Retention Scheduler Tests ─────────────────────────────────────────────────


class FakeSchedulerStore:
    def __init__(self, tenants=None, deleted_count=0):
        self.tenants = tenants or []
        self.deleted_count = deleted_count
        self.purges = []
        self.backend_name = "sqlite"
        self.table_name = "test_events"

    async def distinct_tenants(self):
        return self.tenants

    async def purge_tenant(self, tenant_id, *, window_seconds, batch_size=None):
        self.purges.append((tenant_id, window_seconds, batch_size))
        return self.deleted_count


class RecordingAuditSink:
    def __init__(self, fail=False):
        self.entries = []
        self.fail = fail

    async def record(self, entry: DeletionAuditEntry):
        if self.fail:
            raise RuntimeError("sink error")
        self.entries.append(entry)


@pytest.mark.anyio
async def test_scheduler_unsupported_store():
    with pytest.raises(TypeError, match="RetentionScheduler does not support this store"):
        RetentionScheduler(object(), RetentionPolicy({}), NoOpAuditSink(), 1.0)


@pytest.mark.anyio
async def test_scheduler_validation():
    store = FakeSchedulerStore()
    policy = RetentionPolicy({})
    sink = NoOpAuditSink()

    with pytest.raises(ValueError, match="interval must be a positive number of seconds"):
        RetentionScheduler(store, policy, sink, interval=0.0)

    with pytest.raises(ValueError, match="jitter must be a non-negative number of seconds"):
        RetentionScheduler(store, policy, sink, interval=1.0, jitter=-0.1)

    with pytest.raises(ValueError, match="batch_size must be a positive integer or None"):
        RetentionScheduler(store, policy, sink, interval=1.0, batch_size=0)

    with pytest.raises(TypeError, match="policy must be a RetentionPolicy instance"):
        RetentionScheduler(store, object(), sink, interval=1.0)  # type: ignore

    with pytest.raises(TypeError, match="audit_sink cannot be None"):
        RetentionScheduler(store, policy, None, interval=1.0)  # type: ignore


@pytest.mark.anyio
async def test_scheduler_runs_cycle_purges_and_audits():
    store = FakeSchedulerStore(tenants=["team-a", "team-b", "team-c"], deleted_count=3)
    policy = RetentionPolicy(windows={"team-a": 3600, "team-b": 7200}, default=300)
    sink = RecordingAuditSink()

    scheduler = RetentionScheduler(store, policy, sink, interval=0.01, audit_empty=False)

    await scheduler.start()
    await asyncio.sleep(0.05)
    await scheduler.aclose()

    assert len(store.purges) >= 3
    assert len(sink.entries) >= 3

    entry = sink.entries[0]
    assert entry.deleted_count == 3
    assert entry.backend == "sqlite"
    assert entry.source_table == "test_events"
    if entry.tenant_id == "team-a":
        assert entry.window_seconds == 3600
        assert entry.default_applied is False
    elif entry.tenant_id == "team-c":
        assert entry.window_seconds == 300
        assert entry.default_applied is True


@pytest.mark.anyio
async def test_scheduler_audit_empty_behavior():
    store = FakeSchedulerStore(tenants=["team-a"], deleted_count=0)
    policy = RetentionPolicy(windows={"team-a": 3600})

    sink_false = RecordingAuditSink()
    scheduler_false = RetentionScheduler(store, policy, sink_false, interval=0.01, audit_empty=False)
    await scheduler_false.start()
    await asyncio.sleep(0.03)
    await scheduler_false.aclose()
    assert len(sink_false.entries) == 0

    sink_true = RecordingAuditSink()
    scheduler_true = RetentionScheduler(store, policy, sink_true, interval=0.01, audit_empty=True)
    await scheduler_true.start()
    await asyncio.sleep(0.03)
    await scheduler_true.aclose()
    assert len(sink_true.entries) >= 1
    assert sink_true.entries[0].deleted_count == 0


@pytest.mark.anyio
async def test_scheduler_strict_audit_re_raises():
    store = FakeSchedulerStore(tenants=["team-a"], deleted_count=5)
    policy = RetentionPolicy(windows={"team-a": 3600})
    sink = RecordingAuditSink(fail=True)

    scheduler = RetentionScheduler(store, policy, sink, interval=0.01, strict_audit=True)
    await scheduler.start()
    await asyncio.sleep(0.03)
    await scheduler.aclose()


# ── Config Env Helper Tests ──────────────────────────────────────────────────


def test_retention_policy_from_env_unset():
    assert retention_policy_from_env({}) is None


def test_retention_policy_from_env_valid():
    env = {
        "MCP_PERSIST_RETENTION_WINDOWS": '{"team-a": 3600, "team-b": 7200, "null": 600, "__default__": 1200}',
        "MCP_PERSIST_RETENTION_DEFAULT": "1200",
    }
    policy = retention_policy_from_env(env)
    assert policy is not None
    assert policy.window_for("team-a") == 3600
    assert policy.window_for("team-b") == 7200
    assert policy.window_for(None) == 600
    assert policy.window_for("unknown") == 1200


def test_retention_policy_from_env_validation_errors():
    with pytest.raises(ValueError, match="MCP_PERSIST_RETENTION_DEFAULT must be an integer"):
        retention_policy_from_env({"MCP_PERSIST_RETENTION_DEFAULT": "abc"})

    with pytest.raises(ValueError, match="MCP_PERSIST_RETENTION_WINDOWS must be a valid JSON object"):
        retention_policy_from_env({"MCP_PERSIST_RETENTION_WINDOWS": "{bad_json"})

    with pytest.raises(ValueError, match="MCP_PERSIST_RETENTION_WINDOWS must be a JSON object"):
        retention_policy_from_env({"MCP_PERSIST_RETENTION_WINDOWS": "[1, 2, 3]"})

    with pytest.raises(ValueError, match="disagree"):
        retention_policy_from_env(
            {
                "MCP_PERSIST_RETENTION_WINDOWS": '{"__default__": 600}',
                "MCP_PERSIST_RETENTION_DEFAULT": "1200",
            }
        )

    with pytest.raises(ValueError, match="must be an integer"):
        retention_policy_from_env(
            {
                "MCP_PERSIST_RETENTION_WINDOWS": '{"team-a": "abc"}',
            }
        )
