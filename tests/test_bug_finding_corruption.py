# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false

import asyncio
import time

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import RedisEventStore, SQLiteEventStore

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


@pytest.fixture
async def conn():
    connection = await aiosqlite.connect(":memory:")
    try:
        yield connection
    finally:
        await connection.close()


@pytest.mark.anyio
async def test_sqlite_replay_corrupt_decompression(conn):
    # Initialize SQLite store
    store = SQLiteEventStore(conn, table_name="test_events", ttl=None)
    await store.initialize()

    # 1. Insert a valid anchor event
    anchor_id = await store.store_event("stream-A", SAMPLE_MSG)

    # 2. Insert a corrupt event with gz: prefix directly into the DB
    await conn.execute(
        "INSERT INTO test_events (stream_id, payload, created_at) VALUES (?, ?, ?)",
        ("stream-A", "gz:invalid_base64_payload_!!!", time.time()),
    )
    await conn.commit()

    # 3. Insert another valid event that should be replayed if the corrupt one is skipped
    last_id = await store.store_event("stream-A", SAMPLE_MSG)

    captured = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    # Replay events after anchor_id. If the corrupt event raises an uncaught exception,
    # this will crash!
    try:
        await store.replay_events_after(anchor_id, cb)
        # If it passes, check that we skipped the corrupt event but still replayed the valid one
        assert len(captured) == 1
        assert captured[0].event_id == last_id
    except Exception as e:
        pytest.fail(f"SQLite replay_events_after failed with uncaught exception: {e}")


@pytest.mark.anyio
async def test_sqlite_subscribe_corrupt_decompression(conn):
    # Initialize SQLite store with streaming enabled
    store = SQLiteEventStore(conn, table_name="test_events_sub", ttl=None, enable_streaming=True)
    await store.initialize()

    # We will subscribe first, but SQLite subscribe is forward-only, and polls.
    # So we can write directly to the DB to simulate corruption.
    # Start subscription task
    captured = []

    async def run_sub():
        try:
            async for eid, msg in store.subscribe("stream-B", poll_interval=0.05):
                captured.append((eid, msg))
        except Exception as e:
            captured.append(e)

    sub_task = asyncio.create_task(run_sub())
    await asyncio.sleep(0.1)  # Let the sub seed its last_seen

    # Insert corrupt event directly
    await conn.execute(
        "INSERT INTO test_events_sub (stream_id, payload, created_at) VALUES (?, ?, ?)",
        ("stream-B", "gz:invalid_base64_payload_!!!", time.time()),
    )
    # Insert a valid event afterwards
    await store.store_event("stream-B", SAMPLE_MSG)
    await conn.commit()

    await asyncio.sleep(0.2)
    sub_task.cancel()

    # Check that we didn't receive any exception in the list
    for item in captured:
        if isinstance(item, Exception):
            pytest.fail(f"SQLite subscribe crashed with exception: {item}")


@pytest.mark.anyio
async def test_redis_replay_corrupt_decompression():
    redis_client = fakeredis.FakeRedis()
    store = RedisEventStore(redis_client, key_prefix="mcp_test:", ttl=None)

    # 1. Store anchor event
    anchor_id = await store.store_event("stream-C", SAMPLE_MSG)

    # 2. Store intermediate event to corrupt
    corrupt_id = await store.store_event("stream-C", SAMPLE_MSG)

    # 3. Store another valid event
    last_id = await store.store_event("stream-C", SAMPLE_MSG)

    # Corrupt the intermediate event payload
    await redis_client.hset(f"mcp_test:event:{corrupt_id}", "payload", "gz:invalid_base64_payload_!!!")

    captured = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    # Replay events after anchor_id. If the corrupt event raises an uncaught exception,
    # this will crash!
    try:
        await store.replay_events_after(anchor_id, cb)
        # If it passes, check that we skipped the corrupt event but still replayed the valid one
        assert len(captured) == 1
        assert captured[0].event_id == last_id
    except Exception as e:
        pytest.fail(f"Redis replay_events_after failed with uncaught exception: {e}")


@pytest.mark.anyio
async def test_redis_subscribe_corrupt_decompression():
    redis_client = fakeredis.FakeRedis()
    store = RedisEventStore(redis_client, key_prefix="mcp_test_sub:", ttl=None, enable_streaming=True)

    captured = []

    async def run_sub():
        try:
            async for eid, msg in store.subscribe("stream-D"):
                captured.append((eid, msg))
        except Exception as e:
            captured.append(e)

    # We must seed subscription by subscribing first.
    sub_task = asyncio.create_task(run_sub())
    await asyncio.sleep(0.1)

    # Store a valid event first
    await store.store_event("stream-D", SAMPLE_MSG)

    # Store another valid event to corrupt
    valid_id2 = await store.store_event("stream-D", SAMPLE_MSG)

    # Wait for subscribe to process them or settle
    await asyncio.sleep(0.1)

    # Corrupt the second event's payload
    await redis_client.hset(f"mcp_test_sub:event:{valid_id2}", "payload", "gz:invalid_base64_payload_!!!")

    # Trigger subscription by publishing the corrupt event's ID on the notify channel
    await redis_client.publish("mcp_test_sub:notify:stream-D", valid_id2)

    await asyncio.sleep(0.2)
    sub_task.cancel()

    # Check if subscription crashed
    for item in captured:
        if isinstance(item, Exception):
            pytest.fail(f"Redis subscribe crashed with exception: {item}")


@pytest.mark.anyio
async def test_sqlite_migrate_corrupt_decompression(conn):
    # Initialize SQLite source and destination stores
    source = SQLiteEventStore(conn, table_name="test_events_src", ttl=None)
    dest = SQLiteEventStore(conn, table_name="test_events_dest", ttl=None)
    await source.initialize()
    await dest.initialize()

    # 1. Insert a valid event
    await source.store_event("stream-E", SAMPLE_MSG)

    # 2. Insert a corrupt event with gz: prefix directly into the DB
    await conn.execute(
        "INSERT INTO test_events_src (stream_id, payload, created_at) VALUES (?, ?, ?)",
        ("stream-E", "gz:invalid_base64_payload_!!!", time.time()),
    )
    await conn.commit()

    # 3. Insert another valid event
    await source.store_event("stream-E", SAMPLE_MSG)

    # Migrate. If the corrupt event raises an uncaught exception, this will crash!
    from mcp_persist.migration import migrate

    try:
        result = await migrate(source, dest)
        # Verify that we migrated the 2 valid events and skipped the corrupt one
        assert result.events_migrated == 2
        assert len(result.failed_streams) == 0
    except Exception as e:
        pytest.fail(f"migrate failed with uncaught exception: {e}")
