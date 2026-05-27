# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Tests for SQLiteEventStore.

Uses aiosqlite with an in-memory database — no external server or temp files.
All tests are async (anyio/asyncio backend).
"""

from __future__ import annotations

import asyncio
import logging
import time

import aiosqlite
import pytest
from mcp.server.streamable_http import EventId, EventMessage, StreamId
from mcp.types import JSONRPCRequest

from mcp_persist import SQLiteEventStore

# ── Helpers ───────────────────────────────────────────────────────────────────

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")

TABLE = "test_events"


@pytest.fixture
async def conn():
    connection = await aiosqlite.connect(":memory:")
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
async def store(conn, recwarn):
    s = SQLiteEventStore(conn, table_name=TABLE, ttl=None)
    await s.initialize()
    return s


@pytest.fixture
async def store_with_ttl(conn):
    s = SQLiteEventStore(conn, table_name=TABLE, ttl=60)
    await s.initialize()
    return s


async def collect_events(
    store: SQLiteEventStore,
    last_event_id: EventId,
) -> tuple[list[EventMessage], StreamId | None]:
    captured: list[EventMessage] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    stream_id = await store.replay_events_after(last_event_id, cb)
    return captured, stream_id


async def _age_event(conn, event_id: EventId, seconds_ago: float) -> None:
    """Backdate an event's created_at to simulate elapsed time."""
    await conn.execute(
        f"UPDATE {TABLE} SET created_at = ? WHERE event_id = ?",
        (time.time() - seconds_ago, int(event_id)),
    )
    await conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# store_event tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_store_event_returns_string_integer(store):
    id1 = await store.store_event("stream-A", SAMPLE_MSG)
    assert isinstance(id1, str)
    assert id1.isdigit()


@pytest.mark.anyio
async def test_store_event_ids_are_monotonically_increasing(store):
    id1 = await store.store_event("stream-A", SAMPLE_MSG)
    id2 = await store.store_event("stream-A", SAMPLE_MSG)
    id3 = await store.store_event("stream-B", SAMPLE_MSG)

    assert int(id1) < int(id2) < int(id3)
    assert id1 == "1"


@pytest.mark.anyio
async def test_store_priming_event_writes_empty_payload(store, conn):
    event_id = await store.store_event("stream-A", None)

    async with conn.execute(f"SELECT payload FROM {TABLE} WHERE event_id = ?", (int(event_id),)) as cur:
        row = await cur.fetchone()
    assert row[0] == ""


@pytest.mark.anyio
async def test_store_event_writes_stream_id(store, conn):
    event_id = await store.store_event("my-stream", SAMPLE_MSG)

    async with conn.execute(f"SELECT stream_id FROM {TABLE} WHERE event_id = ?", (int(event_id),)) as cur:
        row = await cur.fetchone()
    assert row[0] == "my-stream"


@pytest.mark.anyio
async def test_store_event_autoinitializes_without_explicit_initialize(conn, recwarn):
    s = SQLiteEventStore(conn, table_name="lazy_events", ttl=None)
    event_id = await s.store_event("stream-A", SAMPLE_MSG)
    assert event_id == "1"


@pytest.mark.anyio
async def test_concurrent_store_event_produces_unique_ids(store):
    tasks = [asyncio.create_task(store.store_event("stream-X", SAMPLE_MSG)) for _ in range(50)]
    ids = await asyncio.gather(*tasks)

    assert len(set(ids)) == 50
    assert all(id_.isdigit() for id_ in ids)


# ─────────────────────────────────────────────────────────────────────────────
# replay_events_after tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_replay_unknown_id_returns_none(store):
    events, stream_id = await collect_events(store, "9999")
    assert stream_id is None
    assert events == []


@pytest.mark.anyio
async def test_replay_returns_correct_stream_id(store):
    anchor = await store.store_event("my-stream", SAMPLE_MSG)

    events, stream_id = await collect_events(store, anchor)
    assert stream_id == "my-stream"
    assert events == []


@pytest.mark.anyio
async def test_replay_skips_priming_events(store):
    anchor = await store.store_event("stream-A", SAMPLE_MSG)
    _ = await store.store_event("stream-A", None)
    id3 = await store.store_event("stream-A", SAMPLE_MSG)

    events, _ = await collect_events(store, anchor)

    assert len(events) == 1
    assert events[0].event_id == id3


@pytest.mark.anyio
async def test_replay_events_are_in_ascending_order(store):
    anchor = await store.store_event("stream-A", SAMPLE_MSG)
    id2 = await store.store_event("stream-A", SAMPLE_MSG)
    id3 = await store.store_event("stream-A", SAMPLE_MSG)

    events, _ = await collect_events(store, anchor)

    assert len(events) == 2
    assert events[0].event_id == id2
    assert events[1].event_id == id3


@pytest.mark.anyio
async def test_replay_excludes_anchor_event_itself(store):
    anchor = await store.store_event("stream-A", SAMPLE_MSG)
    id2 = await store.store_event("stream-A", SAMPLE_MSG)

    events, _ = await collect_events(store, anchor)

    event_ids = [e.event_id for e in events]
    assert anchor not in event_ids
    assert id2 in event_ids


@pytest.mark.anyio
async def test_replay_stream_isolation(store):
    anchor = await store.store_event("stream-A", SAMPLE_MSG)

    _ = await store.store_event("stream-B", SAMPLE_MSG)
    _ = await store.store_event("stream-B", SAMPLE_MSG)

    id4 = await store.store_event("stream-A", SAMPLE_MSG)

    events, stream_id = await collect_events(store, anchor)

    assert stream_id == "stream-A"
    assert len(events) == 1
    assert events[0].event_id == id4


@pytest.mark.anyio
async def test_replay_message_content_round_trips(store):
    original = JSONRPCRequest(jsonrpc="2.0", id="99", method="resources/list")
    anchor = await store.store_event("stream-A", original)
    await store.store_event("stream-A", original)

    events, _ = await collect_events(store, anchor)

    assert len(events) == 1
    replayed = events[0].message
    assert isinstance(replayed.root, JSONRPCRequest)
    assert replayed.root.method == "resources/list"
    assert replayed.root.id == "99"


@pytest.mark.anyio
async def test_replay_event_id_is_attached_to_event_message(store):
    anchor = await store.store_event("stream-A", SAMPLE_MSG)
    id2 = await store.store_event("stream-A", SAMPLE_MSG)

    events, _ = await collect_events(store, anchor)

    assert events[0].event_id == id2


# ─────────────────────────────────────────────────────────────────────────────
# TTL tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_replay_skips_ttl_expired_events(store_with_ttl, conn):
    anchor = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)
    id2 = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)
    id3 = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)

    await _age_event(conn, id2, seconds_ago=120)  # older than the 60s ttl

    events, _ = await collect_events(store_with_ttl, anchor)

    event_ids = [e.event_id for e in events]
    assert id2 not in event_ids
    assert id3 in event_ids


@pytest.mark.anyio
async def test_replay_keeps_events_within_ttl(store_with_ttl):
    anchor = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)
    id2 = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)

    events, _ = await collect_events(store_with_ttl, anchor)

    assert [e.event_id for e in events] == [id2]


@pytest.mark.anyio
async def test_purge_expired_deletes_old_events(store_with_ttl, conn):
    await store_with_ttl.store_event("stream-A", SAMPLE_MSG)
    old_id = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)

    await _age_event(conn, old_id, seconds_ago=120)

    deleted = await store_with_ttl.purge_expired()
    assert deleted == 1

    async with conn.execute(f"SELECT COUNT(*) FROM {TABLE}") as cur:
        (count,) = await cur.fetchone()
    assert count == 1


@pytest.mark.anyio
async def test_purge_expired_is_noop_without_ttl(store):
    await store.store_event("stream-A", SAMPLE_MSG)
    assert await store.purge_expired() == 0


# ─────────────────────────────────────────────────────────────────────────────
# Table-name isolation
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_table_name_isolates_two_stores(conn, recwarn):
    store_a = SQLiteEventStore(conn, table_name="server_a", ttl=None)
    store_b = SQLiteEventStore(conn, table_name="server_b", ttl=None)
    await store_a.initialize()
    await store_b.initialize()

    id_a = await store_a.store_event("stream-1", SAMPLE_MSG)
    id_b = await store_b.store_event("stream-1", SAMPLE_MSG)

    assert id_a == "1"
    assert id_b == "1"

    events_a, stream_id_a = await collect_events(store_a, id_a)
    assert stream_id_a == "stream-1"
    assert events_a == []


def test_invalid_table_name_raises():
    with pytest.raises(ValueError):
        SQLiteEventStore(object(), table_name="bad; DROP TABLE x")


# ─────────────────────────────────────────────────────────────────────────────
# Warning / logging tests
# ─────────────────────────────────────────────────────────────────────────────


def test_no_ttl_emits_log_warning(caplog):
    with caplog.at_level(logging.WARNING, logger="mcp_persist.sqlite"):
        SQLiteEventStore(object(), ttl=None)

    assert any("ttl=None" in record.message for record in caplog.records)


def test_with_ttl_no_warning_emitted(caplog):
    with caplog.at_level(logging.WARNING, logger="mcp_persist.sqlite"):
        SQLiteEventStore(object(), ttl=3600)

    assert not any("ttl" in record.message.lower() for record in caplog.records)
