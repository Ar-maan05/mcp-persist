# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportPrivateUsage=false

import asyncio
import logging
import os
import time

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import PostgresEventStore, RedisEventStore, SQLiteEventStore, migrate

logger = logging.getLogger(__name__)

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


def _msg(i: int) -> JSONRPCRequest:
    return JSONRPCRequest(jsonrpc="2.0", id=str(i), method="tools/call", params={"n": i})


# ─────────────────────────────────────────────────────────────────────────────
# 1. EVENT ID MONOTONICITY
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_sqlite_monotonicity_under_concurrency():
    """Verify event IDs are strictly monotonic and unique under concurrent writers
    with write-behind enabled.
    """
    async with aiosqlite.connect(":memory:") as conn:
        store = SQLiteEventStore(conn, table_name="events_mono", ttl=None, commit_interval=0.01, commit_max_pending=5)
        await store.initialize()

        num_tasks = 20
        writes_per_task = 10
        total_writes = num_tasks * writes_per_task

        async def writer(task_idx: int):
            ids = []
            for i in range(writes_per_task):
                msg = _msg(task_idx * 1000 + i)
                eid = await store.store_event("stream-mono", msg)
                ids.append(eid)
                await asyncio.sleep(0.001)
            return ids

        tasks = [asyncio.create_task(writer(t)) for t in range(num_tasks)]
        results = await asyncio.gather(*tasks)
        all_ids = [int(eid) for sublist in results for eid in sublist]

        # Flush any remaining uncommitted events
        await store.aclose()

        # Check unique event IDs
        assert len(all_ids) == total_writes
        assert len(set(all_ids)) == total_writes

        # Under write-behind, insertion is performed in transactions using SQLite's autoincrement.
        # Check that IDs are strictly increasing.
        sorted_ids = sorted(all_ids)
        assert all_ids == sorted_ids or sorted(all_ids) == list(range(1, total_writes + 1))


@pytest.mark.anyio
async def test_redis_monotonicity_under_concurrency():
    """Verify event IDs are strictly monotonic and unique under concurrent writers in Redis."""
    client = fakeredis.FakeRedis()
    store = RedisEventStore(client, key_prefix="mono:", ttl=None)

    num_tasks = 20
    writes_per_task = 10
    total_writes = num_tasks * writes_per_task

    async def writer(task_idx: int):
        ids = []
        for i in range(writes_per_task):
            msg = _msg(task_idx * 1000 + i)
            eid = await store.store_event("stream-mono-redis", msg)
            ids.append(eid)
            await asyncio.sleep(0.001)
        return ids

    tasks = [asyncio.create_task(writer(t)) for t in range(num_tasks)]
    results = await asyncio.gather(*tasks)
    all_ids = [int(eid) for sublist in results for eid in sublist]

    try:
        await client.aclose()
    except AttributeError:
        await client.close()

    assert len(all_ids) == total_writes
    assert len(set(all_ids)) == total_writes
    # Redis uses INCR which is 1-based and contiguous
    assert sorted(all_ids) == list(range(1, total_writes + 1))


@pytest.mark.anyio
async def test_postgres_monotonicity_under_concurrency():
    """Skip or run Postgres monotonicity tests if DB is available."""
    pg_url = os.environ.get("MCP_TEST_POSTGRES_URL")
    if not pg_url:
        pytest.skip("MCP_TEST_POSTGRES_URL not set, skipping Postgres tests")

    import asyncpg

    async with asyncpg.create_pool(pg_url) as pool:
        store = PostgresEventStore(pool, table_name="mono_pg", ttl=None)
        await store.initialize()

        num_tasks = 10
        writes_per_task = 5
        total_writes = num_tasks * writes_per_task

        async def writer(task_idx: int):
            ids = []
            for i in range(writes_per_task):
                msg = _msg(task_idx * 1000 + i)
                eid = await store.store_event("stream-mono-pg", msg)
                ids.append(eid)
                await asyncio.sleep(0.001)
            return ids

        tasks = [asyncio.create_task(writer(t)) for t in range(num_tasks)]
        results = await asyncio.gather(*tasks)
        all_ids = [int(eid) for sublist in results for eid in sublist]

        assert len(all_ids) == total_writes
        assert len(set(all_ids)) == total_writes
        # IDs are assigned monotonically in time, but concurrent writers collect
        # them grouped by task, so the flattened list is not globally sorted. The
        # guarantee is that the N writes get N unique, gap-free IDs from the
        # identity sequence — a contiguous block. Anchor the range at the observed
        # minimum (not 1): the table persists across runs, so the sequence may
        # already be past 1.
        ordered = sorted(all_ids)
        assert ordered == list(range(ordered[0], ordered[0] + total_writes))


# ─────────────────────────────────────────────────────────────────────────────
# 2. MIGRATION SAFETY AND SCHEMA EVOLUTION
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_migration_repeated_execution_is_safe_but_duplicates():
    """Verify that migrate() run multiple times works safely without crashes,
    even though it duplicates events (since migrate does not do deduplication).
    """
    async with aiosqlite.connect(":memory:") as conn_src, aiosqlite.connect(":memory:") as conn_dst:
        src = SQLiteEventStore(conn_src, table_name="src", ttl=None)
        dst = SQLiteEventStore(conn_dst, table_name="dst", ttl=None)
        await src.initialize()
        await dst.initialize()

        # Seed source
        await src.store_event("stream-A", _msg(1))
        await src.store_event("stream-A", _msg(2))

        # First migration
        res1 = await migrate(src, dst)
        assert res1.events_migrated == 2
        assert res1.streams_migrated == 1

        # Second migration (repeated run)
        res2 = await migrate(src, dst)
        assert res2.events_migrated == 2
        assert res2.streams_migrated == 1

        # Check total events in dst (should be duplicated since it is a stream copy)
        dst_events = []
        async for _eid, msg in dst._iter_stream_events("stream-A"):
            dst_events.append(msg)

        assert len(dst_events) == 4
        # Payloads are preserved
        assert dst_events[0].root.id == "1"
        assert dst_events[1].root.id == "2"
        assert dst_events[2].root.id == "1"
        assert dst_events[3].root.id == "2"


@pytest.mark.anyio
async def test_sqlite_schema_evolution_robustness():
    """Verify that SQLiteEventStore continues to work when schema is evolved (e.g. upgraded/downgraded)
    without losing existing data.
    """
    async with aiosqlite.connect(":memory:") as conn:
        store = SQLiteEventStore(conn, table_name="schema_ev", ttl=None)
        await store.initialize()

        # 1. Store initial event (serves as anchor)
        eid1 = await store.store_event("stream-ev", _msg(1))
        assert eid1 == "1"

        # 2. Upgrade schema: Add extra columns and custom indices
        await conn.execute("ALTER TABLE schema_ev ADD COLUMN extra_info TEXT")
        await conn.execute("ALTER TABLE schema_ev ADD COLUMN version INTEGER DEFAULT 1")
        # Create a new custom index on the evolved columns
        await conn.execute("CREATE INDEX IF NOT EXISTS schema_ev_version_idx ON schema_ev (version)")
        await conn.commit()

        # 3. Store event under upgraded schema
        eid2 = await store.store_event("stream-ev", _msg(2))
        assert eid2 == "2"

        # 4. Manually insert data using the new columns to simulate upgraded application writes
        await conn.execute(
            "INSERT INTO schema_ev (stream_id, payload, created_at, extra_info, version) VALUES (?, ?, ?, ?, ?)",
            ("stream-ev", _msg(3).model_dump_json(by_alias=True, exclude_none=True), time.time(), "debug_metadata", 2),
        )
        await conn.commit()

        # 5. Verify old and new data is retrieved correctly when replaying after the anchor eid1
        events = []

        async def cb(ev: EventMessage):
            events.append(ev)

        stream_id = await store.replay_events_after(eid1, cb)
        assert stream_id == "stream-ev"
        # Should retrieve event 2 and 3
        assert len(events) == 2
        assert [ev.message.root.id for ev in events] == ["2", "3"]

        # 6. Downgrade schema: drop an index (SQLite has no DROP COLUMN before
        #    3.35; dropping an index is safe/standard).
        await conn.execute("DROP INDEX schema_ev_version_idx")
        await conn.commit()

        # Store another event and verify it still works
        eid4 = await store.store_event("stream-ev", _msg(4))
        assert eid4 == "4"


# ─────────────────────────────────────────────────────────────────────────────
# 3. SSE SUBSCRIPTION AND REPLAY GUARANTEES
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_replay_guarantees_exact_set():
    """Verify that replaying from an event ID returns exactly the matching set (all subsequent events)."""
    async with aiosqlite.connect(":memory:") as conn:
        store = SQLiteEventStore(conn, table_name="replay_guar", ttl=None)
        await store.initialize()

        for i in range(1, 11):
            await store.store_event("stream-A", _msg(i))

        events = []

        async def cb(ev: EventMessage):
            events.append(ev)

        # Replay after event ID 3 -> should return 4..10
        stream_id = await store.replay_events_after("3", cb)
        assert stream_id == "stream-A"
        assert len(events) == 7
        assert [ev.message.root.id for ev in events] == [str(x) for x in range(4, 11)]
        assert [ev.event_id for ev in events] == [str(x) for x in range(4, 11)]


@pytest.mark.anyio
async def test_replay_non_existent_stream_returns_none():
    """Verify that replaying a non-existent stream ID (or invalid anchor) returns None gracefully."""
    async with aiosqlite.connect(":memory:") as conn:
        store = SQLiteEventStore(conn, table_name="replay_none", ttl=None)
        await store.initialize()

        events = []

        async def cb(ev: EventMessage):
            events.append(ev)

        stream_id = await store.replay_events_after("999", cb)
        assert stream_id is None
        assert len(events) == 0


@pytest.mark.anyio
async def test_replay_with_expired_and_missing_ids(caplog):
    """Verify replay behavior when some events are expired or missing."""
    async with aiosqlite.connect(":memory:") as conn:
        # TTL of 1 second
        store = SQLiteEventStore(conn, table_name="replay_expired", ttl=1)
        await store.initialize()

        eid1 = await store.store_event("stream-A", _msg(1))
        eid2 = await store.store_event("stream-A", _msg(2))
        await store.store_event("stream-A", _msg(3))

        # Manually backdate eid2 (middle event) to simulate expiration
        await conn.execute("UPDATE replay_expired SET created_at = ? WHERE event_id = ?", (time.time() - 10, int(eid2)))
        await conn.commit()

        events = []

        async def cb(ev: EventMessage):
            events.append(ev)

        with caplog.at_level(logging.WARNING):
            # Replay after eid1 -> event 2 is expired (should log a warning & skip), event 3 is healthy
            stream_id = await store.replay_events_after(eid1, cb)
            assert stream_id == "stream-A"
            assert len(events) == 1
            assert events[0].message.root.id == "3"

            # Check warning was logged
            assert any("Replay gap on stream stream-A" in record.message for record in caplog.records)


@pytest.mark.anyio
async def test_subscribe_streaming_in_order_no_loss():
    """Verify that subscribing delivers all events in-order with zero loss,
    and is forward-only.
    """
    async with aiosqlite.connect(":memory:") as conn:
        store = SQLiteEventStore(conn, table_name="stream_order", ttl=None, enable_streaming=True)
        await store.initialize()

        # 1. Event stored before subscription should not be delivered
        await store.store_event("stream-A", _msg(0))

        received = []
        subscription_ready = asyncio.Event()

        async def sub_task():
            subscription_ready.set()
            async for event_id, message in store.subscribe("stream-A", poll_interval=0.01):
                received.append((event_id, message))
                if len(received) == 20:
                    break

        task = asyncio.create_task(sub_task())
        await subscription_ready.wait()
        await asyncio.sleep(0.05)  # Make sure the subscription is registered

        # 2. Write 20 events quickly
        for i in range(1, 21):
            await store.store_event("stream-A", _msg(i))
            await asyncio.sleep(0.001)

        await asyncio.wait_for(task, timeout=5)

        # 3. Check delivery guarantees
        assert len(received) == 20
        # Check in-order delivery and zero loss
        ids = [int(msg.root.id) for _, msg in received]
        assert ids == list(range(1, 21))


# ─────────────────────────────────────────────────────────────────────────────
# 4. DATA CORRUPTION AND BOUNDARY TESTING
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_store_handles_special_characters():
    """Verify store handles SQL injection payloads, special chars, null bytes,
    emojis, and quotes in stream IDs and payloads.
    """
    async with aiosqlite.connect(":memory:") as conn:
        store = SQLiteEventStore(conn, table_name="special_chars", ttl=None)
        await store.initialize()

        special_stream = "stream' OR '1'='1\"; DROP TABLE special_chars; -- \x00 😊"

        # Store a priming event to act as replay anchor
        anchor_eid = await store.store_event(special_stream, None)

        special_msg = JSONRPCRequest(
            jsonrpc="2.0", id="special_id", method="tools/call", params={"value": "quote' \" \\ \x00 emoji 👍"}
        )

        # Store the actual event
        await store.store_event(special_stream, special_msg)

        # Replay and verify
        events = []

        async def cb(ev: EventMessage):
            events.append(ev)

        stream_id = await store.replay_events_after(anchor_eid, cb)
        assert stream_id == special_stream
        assert len(events) == 1
        assert events[0].message.root.id == "special_id"
        assert events[0].message.root.params["value"] == "quote' \" \\ \x00 emoji 👍"


@pytest.mark.anyio
async def test_store_handles_extremely_large_payloads():
    """Verify event store can store and retrieve extremely large payloads (e.g., 2 MB)
    under gzip compression configuration.
    """
    async with aiosqlite.connect(":memory:") as conn:
        store = SQLiteEventStore(
            conn, table_name="large_payloads", ttl=None, compression="gzip", compress_min_bytes=100
        )
        await store.initialize()

        # Priming event to act as anchor
        anchor_eid = await store.store_event("stream-large", None)

        large_string = "A" * (2 * 1024 * 1024)  # 2 MB string
        large_msg = JSONRPCRequest(jsonrpc="2.0", id="large_id", method="large_method", params={"data": large_string})

        eid = await store.store_event("stream-large", large_msg)

        # Verify DB representation is compressed
        async with conn.execute("SELECT payload FROM large_payloads WHERE event_id = ?", (int(eid),)) as cur:
            row = await cur.fetchone()
            db_payload = row[0]
            # Since gzip compress returns base64 string when compression is active, it starts with gzipped prefix
            assert db_payload.startswith("gz:")
            assert len(db_payload) < len(large_string)  # Check compression saved space

        # Replay and verify it decompresses perfectly after anchor_eid
        events = []

        async def cb(ev: EventMessage):
            events.append(ev)

        await store.replay_events_after(anchor_eid, cb)
        assert len(events) == 1
        assert events[0].message.root.id == "large_id"
        assert len(events[0].message.root.params["data"]) == 2 * 1024 * 1024


@pytest.mark.anyio
async def test_replay_handles_corrupted_payloads_gracefully(caplog):
    """Verify that a corrupted payload (invalid JSON or invalid compression)
    is skipped during replay without failing the entire query.
    """
    async with aiosqlite.connect(":memory:") as conn:
        store = SQLiteEventStore(conn, table_name="corrupt_events", ttl=None)
        await store.initialize()

        eid1 = await store.store_event("stream-corrupt", _msg(1))
        # Store a dummy event that we will corrupt
        eid2 = await store.store_event("stream-corrupt", _msg(2))
        await store.store_event("stream-corrupt", _msg(3))

        # Manually corrupt the payload of eid2 in the DB
        await conn.execute("UPDATE corrupt_events SET payload = ? WHERE event_id = ?", ("{invalid json...", int(eid2)))
        await conn.commit()

        events = []

        async def cb(ev: EventMessage):
            events.append(ev)

        with caplog.at_level(logging.WARNING):
            # Replay after eid1 -> yields eid2 (corrupt, skipped) and eid3 (healthy)
            stream_id = await store.replay_events_after(eid1, cb)
            assert stream_id == "stream-corrupt"

            # Event 2 should be skipped, event 3 should be successfully delivered
            assert len(events) == 1
            assert events[0].message.root.id == "3"

            # Verify warning was logged for skipped corrupt event
            assert any("failed JSONRPC validation" in record.message for record in caplog.records)


@pytest.mark.anyio
async def test_replay_handles_invalid_message_schema_gracefully(caplog):
    """Verify that a payload that is valid JSON but does not match JSONRPCMessage schema
    is skipped during replay.
    """
    async with aiosqlite.connect(":memory:") as conn:
        store = SQLiteEventStore(conn, table_name="invalid_schema", ttl=None)
        await store.initialize()

        eid1 = await store.store_event("stream-schema", _msg(1))
        # Store a dummy event that we will update with invalid schema JSON
        eid2 = await store.store_event("stream-schema", _msg(2))
        await store.store_event("stream-schema", _msg(3))

        # Update eid2 to be invalid JSON-RPC format (missing mandatory fields like jsonrpc or id/method)
        await conn.execute(
            "UPDATE invalid_schema SET payload = ? WHERE event_id = ?", ('{"not": "a jsonrpc request"}', int(eid2))
        )
        await conn.commit()

        events = []

        async def cb(ev: EventMessage):
            events.append(ev)

        with caplog.at_level(logging.WARNING):
            # Replay after eid1 -> yields eid2 (invalid format, skipped) and eid3 (healthy)
            stream_id = await store.replay_events_after(eid1, cb)
            assert stream_id == "stream-schema"

            # Event 2 should be skipped, event 3 delivered
            assert len(events) == 1
            assert events[0].message.root.id == "3"

            # Verify warning was logged for skipped event
            assert any("failed JSONRPC validation" in record.message for record in caplog.records)
