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
    # NOTE: this passes because aiosqlite funnels every statement through a
    # single background thread on one connection, so these "concurrent" stores
    # are actually serialized. It does NOT show that SQLite is safe for
    # concurrent writes from multiple connections or processes — it is not.
    # Use RedisEventStore or PostgresEventStore for genuine multi-writer setups.
    tasks = [asyncio.create_task(store.store_event("stream-X", SAMPLE_MSG)) for _ in range(50)]
    ids = await asyncio.gather(*tasks)

    assert len(set(ids)) == 50
    assert all(id_.isdigit() for id_ in ids)


@pytest.mark.anyio
async def test_replay_while_writing_stress(store):
    # Establish a starting anchor
    start_id = await store.store_event("stress-stream", SAMPLE_MSG)

    num_events = 100
    write_done = asyncio.Event()

    async def writer():
        for i in range(num_events):
            msg = JSONRPCRequest(jsonrpc="2.0", id=f"stress-{i}", method="stress")
            await store.store_event("stress-stream", msg)
            await asyncio.sleep(0.001)
        write_done.set()

    replayed_ids: list[str] = []

    async def reader():
        current_anchor = start_id
        while True:
            captured: list[EventMessage] = []

            async def cb(event: EventMessage) -> None:
                captured.append(event)

            await store.replay_events_after(current_anchor, cb)
            if captured:
                for ev in captured:
                    if ev.event_id is not None:
                        replayed_ids.append(ev.event_id)
                last_id = captured[-1].event_id
                if last_id is not None:
                    current_anchor = last_id

            if write_done.is_set() and len(replayed_ids) >= num_events:
                break
            await asyncio.sleep(0.002)

    await asyncio.gather(writer(), reader())

    assert len(replayed_ids) == num_events
    # Check that IDs are unique and monotonically increasing
    assert len(set(replayed_ids)) == num_events
    assert replayed_ids == sorted(replayed_ids, key=int)


# ─────────────────────────────────────────────────────────────────────────────
# replay_events_after tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_replay_unknown_id_returns_none(store):
    events, stream_id = await collect_events(store, "9999")
    assert stream_id is None
    assert events == []


@pytest.mark.anyio
async def test_replay_non_numeric_event_id_returns_none(store):
    # Last-Event-ID is a client-controlled header; a non-numeric value must be
    # handled gracefully (return None, no ValueError/traceback).
    events, stream_id = await collect_events(store, "not-a-number")
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
async def test_replay_skips_corrupt_payload_without_aborting_stream(store, conn, caplog):
    # A single corrupt payload must not poison the whole stream: events both
    # before and after the bad one must still reach the client on reconnect.
    anchor = await store.store_event("stream-A", SAMPLE_MSG)
    good_before = await store.store_event("stream-A", SAMPLE_MSG)
    bad = await store.store_event("stream-A", SAMPLE_MSG)
    good_after = await store.store_event("stream-A", SAMPLE_MSG)

    # Corrupt the middle event's payload directly, bypassing store_event().
    await conn.execute(
        f"UPDATE {TABLE} SET payload = ? WHERE event_id = ?",
        ("{not valid json", int(bad)),
    )
    await conn.commit()

    with caplog.at_level(logging.WARNING):
        events, stream_id = await collect_events(store, anchor)

    assert [e.event_id for e in events] == [good_before, good_after]
    assert stream_id == "stream-A"
    assert "failed JSONRPC validation" in caplog.text


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


@pytest.mark.anyio
async def test_table_name_with_hyphens(conn):
    store = SQLiteEventStore(conn, table_name="mcp-events-table", ttl=None)
    await store.initialize()
    event_id = await store.store_event("stream-A", SAMPLE_MSG)
    assert event_id == "1"

    events, stream_id = await collect_events(store, event_id)
    assert stream_id == "stream-A"
    assert events == []


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


# ─────────────────────────────────────────────────────────────────────────────
# Schema qualification & Timeout tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_schema_qualified_table_name(conn):
    store = SQLiteEventStore(conn, table_name="main.schema_events", ttl=None)
    await store.initialize()
    event_id = await store.store_event("stream-A", SAMPLE_MSG)
    assert event_id == "1"

    events, stream_id = await collect_events(store, event_id)
    assert stream_id == "stream-A"
    assert events == []


@pytest.mark.anyio
async def test_sqlite_timeout_applied(conn):
    store = SQLiteEventStore(conn, table_name="timeout_events", timeout=5.0)
    await store.initialize()
    async with conn.execute("PRAGMA busy_timeout") as cursor:
        row = await cursor.fetchone()
    assert row[0] == 5000


# ─────────────────────────────────────────────────────────────────────────────
# ping() / compression / batched purge / replay gap (1.4.0)
# ─────────────────────────────────────────────────────────────────────────────


def _big_msg(marker: str, size: int = 5000) -> JSONRPCRequest:
    return JSONRPCRequest(jsonrpc="2.0", id="1", method="big", params={"data": marker * size})


@pytest.mark.anyio
async def test_ping_returns_true(store):
    assert await store.ping() is True


@pytest.mark.anyio
async def test_compression_roundtrips_large_payload(conn):
    store = SQLiteEventStore(conn, table_name=TABLE, ttl=None, compression="gzip", compress_min_bytes=100)
    await store.initialize()

    anchor = await store.store_event("s", SAMPLE_MSG)
    eid = await store.store_event("s", _big_msg("x"))

    async with conn.execute(f"SELECT payload FROM {TABLE} WHERE event_id = ?", (int(eid),)) as cur:
        (stored,) = await cur.fetchone()
    assert stored.startswith("gz:")
    assert len(stored) < 5000  # actually compressed, not just marked

    events, _ = await collect_events(store, anchor)
    assert [e.event_id for e in events] == [eid]
    assert events[0].message.root.params == {"data": "x" * 5000}


@pytest.mark.anyio
async def test_compression_skips_small_payload(conn):
    store = SQLiteEventStore(conn, table_name=TABLE, ttl=None, compression="gzip", compress_min_bytes=100000)
    await store.initialize()

    eid = await store.store_event("s", SAMPLE_MSG)
    async with conn.execute(f"SELECT payload FROM {TABLE} WHERE event_id = ?", (int(eid),)) as cur:
        (stored,) = await cur.fetchone()
    assert not stored.startswith("gz:")
    assert stored.startswith("{")


@pytest.mark.anyio
async def test_uncompressed_store_reads_compressed_payload(conn):
    writer = SQLiteEventStore(conn, table_name=TABLE, ttl=None, compression="gzip", compress_min_bytes=100)
    await writer.initialize()
    anchor = await writer.store_event("s", SAMPLE_MSG)
    await writer.store_event("s", _big_msg("y", 3000))

    reader = SQLiteEventStore(conn, table_name=TABLE, ttl=None)  # compression disabled
    events, _ = await collect_events(reader, anchor)
    assert events[0].message.root.params == {"data": "y" * 3000}


def test_invalid_compression_codec_raises():
    with pytest.raises(ValueError):
        SQLiteEventStore(object(), compression="zstd")


def test_negative_compress_min_bytes_raises():
    with pytest.raises(ValueError):
        SQLiteEventStore(object(), compression="gzip", compress_min_bytes=-1)


@pytest.mark.anyio
async def test_purge_expired_batched_deletes_all(store_with_ttl, conn):
    ids = [await store_with_ttl.store_event("s", SAMPLE_MSG) for _ in range(5)]
    for i in ids:
        await _age_event(conn, i, seconds_ago=120)

    removed = await store_with_ttl.purge_expired(batch_size=2)
    assert removed == 5

    async with conn.execute(f"SELECT COUNT(*) FROM {TABLE}") as cur:
        (count,) = await cur.fetchone()
    assert count == 0


@pytest.mark.anyio
async def test_purge_expired_batched_leaves_live_events(store_with_ttl, conn):
    old = await store_with_ttl.store_event("s", SAMPLE_MSG)
    fresh = await store_with_ttl.store_event("s", SAMPLE_MSG)
    await _age_event(conn, old, seconds_ago=120)

    removed = await store_with_ttl.purge_expired(batch_size=1)
    assert removed == 1

    async with conn.execute(f"SELECT event_id FROM {TABLE}") as cur:
        rows = await cur.fetchall()
    assert [str(r[0]) for r in rows] == [fresh]


@pytest.mark.anyio
async def test_purge_expired_rejects_bad_batch_size(store_with_ttl):
    with pytest.raises(ValueError):
        await store_with_ttl.purge_expired(batch_size=0)


@pytest.mark.anyio
async def test_replay_gap_logs_warning_for_expired(store_with_ttl, conn, caplog):
    anchor = await store_with_ttl.store_event("s", SAMPLE_MSG)
    expired = await store_with_ttl.store_event("s", SAMPLE_MSG)
    await _age_event(conn, expired, seconds_ago=120)

    with caplog.at_level(logging.WARNING, logger="mcp_persist.sqlite"):
        events, _ = await collect_events(store_with_ttl, anchor)

    assert [e.event_id for e in events] == []
    assert "Replay gap" in caplog.text


@pytest.mark.anyio
async def test_replay_no_gap_warning_when_all_live(store_with_ttl, caplog):
    anchor = await store_with_ttl.store_event("s", SAMPLE_MSG)
    await store_with_ttl.store_event("s", SAMPLE_MSG)

    with caplog.at_level(logging.WARNING, logger="mcp_persist.sqlite"):
        await collect_events(store_with_ttl, anchor)

    assert "Replay gap" not in caplog.text


@pytest.mark.anyio
async def test_replay_no_gap_warning_for_expired_priming_event(store_with_ttl, conn, caplog):
    # An expired priming event (empty payload) is never replayed, so it is not a
    # client-visible gap and must not trigger the warning.
    anchor = await store_with_ttl.store_event("s", SAMPLE_MSG)
    priming = await store_with_ttl.store_event("s", None)
    await _age_event(conn, priming, seconds_ago=120)

    with caplog.at_level(logging.WARNING, logger="mcp_persist.sqlite"):
        events, _ = await collect_events(store_with_ttl, anchor)

    assert [e.event_id for e in events] == []
    assert "Replay gap" not in caplog.text
