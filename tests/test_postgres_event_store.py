# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Tests for PostgresEventStore.

These require a real PostgreSQL server — there is no in-process fake. They run
only when MCP_TEST_POSTGRES_URL is set (e.g. postgresql://postgres@localhost:5432/postgres);
otherwise the whole module is skipped, so local development without Postgres is
unaffected. CI provides a Postgres service container and sets the variable.

Each test drops and recreates its table, which also resets the IDENTITY counter,
so counter-based assertions (id == "1") hold without wiping a shared database.

All DB tests are async (anyio/asyncio backend).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

import pytest
from mcp.server.streamable_http import EventId, EventMessage, StreamId
from mcp.types import JSONRPCRequest

from mcp_persist import PostgresEventStore

# ── Helpers ───────────────────────────────────────────────────────────────────

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")

TABLE = "test_events"

POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL")

pytestmark = pytest.mark.skipif(
    POSTGRES_URL is None,
    reason="set MCP_TEST_POSTGRES_URL to run Postgres tests",
)


@pytest.fixture
async def pg_pool():
    import asyncpg

    pool = await asyncpg.create_pool(POSTGRES_URL)
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def clean_table(pg_pool):
    # Drop before and after so each test gets a pristine table with the
    # IDENTITY sequence reset to 1.
    await pg_pool.execute(f"DROP TABLE IF EXISTS {TABLE}")
    try:
        yield
    finally:
        await pg_pool.execute(f"DROP TABLE IF EXISTS {TABLE}")


@pytest.fixture
async def store(pg_pool, clean_table, recwarn):
    s = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None)
    await s.initialize()
    return s


@pytest.fixture
async def store_with_ttl(pg_pool, clean_table):
    s = PostgresEventStore(pg_pool, table_name=TABLE, ttl=60)
    await s.initialize()
    return s


async def collect_events(
    store: PostgresEventStore,
    last_event_id: EventId,
) -> tuple[list[EventMessage], StreamId | None]:
    captured: list[EventMessage] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    stream_id = await store.replay_events_after(last_event_id, cb)
    return captured, stream_id


async def _age_event(pool, event_id: EventId, seconds_ago: float) -> None:
    """Backdate an event's created_at to simulate elapsed time."""
    await pool.execute(
        f"UPDATE {TABLE} SET created_at = $1 WHERE event_id = $2",
        time.time() - seconds_ago,
        int(event_id),
    )


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
async def test_store_priming_event_writes_empty_payload(store, pg_pool):
    event_id = await store.store_event("stream-A", None)

    payload = await pg_pool.fetchval(f"SELECT payload FROM {TABLE} WHERE event_id = $1", int(event_id))
    assert payload == ""


@pytest.mark.anyio
async def test_store_event_writes_stream_id(store, pg_pool):
    event_id = await store.store_event("my-stream", SAMPLE_MSG)

    stream_id = await pg_pool.fetchval(f"SELECT stream_id FROM {TABLE} WHERE event_id = $1", int(event_id))
    assert stream_id == "my-stream"


@pytest.mark.anyio
async def test_store_event_autoinitializes_without_explicit_initialize(pg_pool, clean_table, recwarn):
    s = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None)
    event_id = await s.store_event("stream-A", SAMPLE_MSG)
    assert event_id == "1"


@pytest.mark.anyio
async def test_concurrent_store_event_produces_unique_ids(store):
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
async def test_replay_skips_corrupt_payload_without_aborting_stream(store, pg_pool, caplog):
    # A single corrupt payload must not poison the whole stream: events both
    # before and after the bad one must still reach the client on reconnect.
    anchor = await store.store_event("stream-A", SAMPLE_MSG)
    good_before = await store.store_event("stream-A", SAMPLE_MSG)
    bad = await store.store_event("stream-A", SAMPLE_MSG)
    good_after = await store.store_event("stream-A", SAMPLE_MSG)

    # Corrupt the middle event's payload directly, bypassing store_event().
    await pg_pool.execute(
        f"UPDATE {TABLE} SET payload = $1 WHERE event_id = $2",
        "{not valid json",
        int(bad),
    )

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
async def test_replay_skips_ttl_expired_events(store_with_ttl, pg_pool):
    anchor = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)
    id2 = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)
    id3 = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)

    await _age_event(pg_pool, id2, seconds_ago=120)  # older than the 60s ttl

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
async def test_purge_expired_deletes_old_events(store_with_ttl, pg_pool):
    await store_with_ttl.store_event("stream-A", SAMPLE_MSG)
    old_id = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)

    await _age_event(pg_pool, old_id, seconds_ago=120)

    deleted = await store_with_ttl.purge_expired()
    assert deleted == 1

    count = await pg_pool.fetchval(f"SELECT COUNT(*) FROM {TABLE}")
    assert count == 1


@pytest.mark.anyio
async def test_purge_expired_is_noop_without_ttl(store):
    await store.store_event("stream-A", SAMPLE_MSG)
    assert await store.purge_expired() == 0


# ─────────────────────────────────────────────────────────────────────────────
# Table-name isolation
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_table_name_isolates_two_stores(pg_pool, recwarn):
    for t in ("server_a", "server_b"):
        await pg_pool.execute(f"DROP TABLE IF EXISTS {t}")
    try:
        store_a = PostgresEventStore(pg_pool, table_name="server_a", ttl=None)
        store_b = PostgresEventStore(pg_pool, table_name="server_b", ttl=None)
        await store_a.initialize()
        await store_b.initialize()

        id_a = await store_a.store_event("stream-1", SAMPLE_MSG)
        id_b = await store_b.store_event("stream-1", SAMPLE_MSG)

        assert id_a == "1"
        assert id_b == "1"

        events_a, stream_id_a = await collect_events(store_a, id_a)
        assert stream_id_a == "stream-1"
        assert events_a == []
    finally:
        for t in ("server_a", "server_b"):
            await pg_pool.execute(f"DROP TABLE IF EXISTS {t}")


def test_invalid_table_name_raises():
    with pytest.raises(ValueError):
        PostgresEventStore(object(), table_name="bad; DROP TABLE x")


@pytest.mark.anyio
async def test_table_name_with_hyphens(pg_pool):
    table_name = "mcp-events-table"
    await pg_pool.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    try:
        store = PostgresEventStore(pg_pool, table_name=table_name, ttl=None)
        await store.initialize()
        event_id = await store.store_event("stream-A", SAMPLE_MSG)
        assert event_id == "1"

        events, stream_id = await collect_events(store, event_id)
        assert stream_id == "stream-A"
        assert events == []
    finally:
        await pg_pool.execute(f'DROP TABLE IF EXISTS "{table_name}"')


# ─────────────────────────────────────────────────────────────────────────────
# Warning / logging tests
# ─────────────────────────────────────────────────────────────────────────────


def test_no_ttl_emits_log_warning(caplog):
    with caplog.at_level(logging.WARNING, logger="mcp_persist.postgres"):
        PostgresEventStore(object(), ttl=None)

    assert any("ttl=None" in record.message for record in caplog.records)


def test_with_ttl_no_warning_emitted(caplog):
    with caplog.at_level(logging.WARNING, logger="mcp_persist.postgres"):
        PostgresEventStore(object(), ttl=3600)

    assert not any("ttl" in record.message.lower() for record in caplog.records)


# ─────────────────────────────────────────────────────────────────────────────
# Schema qualification, Timeout, and Pagination tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_schema_qualified_table_name(pg_pool):
    # public is the default schema in Postgres
    table_name = "public.schema_events"
    await pg_pool.execute(f"DROP TABLE IF EXISTS {table_name}")
    try:
        store = PostgresEventStore(pg_pool, table_name=table_name, ttl=None)
        await store.initialize()
        event_id = await store.store_event("stream-A", SAMPLE_MSG)
        assert event_id == "1"

        events, stream_id = await collect_events(store, event_id)
        assert stream_id == "stream-A"
        assert events == []
    finally:
        await pg_pool.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.anyio
async def test_postgres_timeout_applied(pg_pool, clean_table):
    store = PostgresEventStore(pg_pool, table_name=TABLE, timeout=5.0)
    await store.initialize()
    # Storing an event should succeed and use the timeout
    event_id = await store.store_event("stream-A", SAMPLE_MSG)
    assert event_id == "1"


@pytest.mark.anyio
async def test_replay_pagination_batches(pg_pool, clean_table):
    # replay_batch_size=2 with more events than that forces multiple round-trips,
    # exercising the pagination loop — via the public kwarg, no monkey-patching.
    store = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None, replay_batch_size=2)
    await store.initialize()
    anchor = await store.store_event("stream-A", SAMPLE_MSG)
    ids = [await store.store_event("stream-A", SAMPLE_MSG) for _ in range(5)]

    events, stream_id = await collect_events(store, anchor)
    assert stream_id == "stream-A"
    assert [e.event_id for e in events] == ids


@pytest.mark.anyio
async def test_replay_batch_size_must_be_positive(pg_pool, clean_table):
    with pytest.raises(ValueError, match="replay_batch_size"):
        PostgresEventStore(pg_pool, table_name=TABLE, replay_batch_size=0)


# ── Metrics hooks (parity with test_metrics.py) ───────────────────────────────


class _RecordingCollector:
    def __init__(self) -> None:
        self.store_calls: list[tuple] = []
        self.replay_calls: list[tuple] = []
        self.errors: list[tuple] = []

    def on_store_event(self, stream_id, event_id, duration_ms) -> None:
        self.store_calls.append((stream_id, event_id, duration_ms))

    def on_replay(self, stream_id, events_replayed, duration_ms) -> None:
        self.replay_calls.append((stream_id, events_replayed, duration_ms))

    def on_error(self, operation, error) -> None:
        self.errors.append((operation, error))


@pytest.mark.anyio
async def test_metrics_fire_on_store_and_replay(pg_pool, clean_table):
    collector = _RecordingCollector()
    store = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None, metrics=collector)
    await store.initialize()

    anchor = await store.store_event("stream-A", None)
    event_id = await store.store_event("stream-A", SAMPLE_MSG)

    events, stream_id = await collect_events(store, anchor)
    assert stream_id == "stream-A"
    assert [e.event_id for e in events] == [event_id]

    assert len(collector.store_calls) == 2
    assert collector.store_calls[1][0] == "stream-A"
    assert collector.store_calls[1][1] == event_id
    assert isinstance(collector.store_calls[1][2], float)
    assert len(collector.replay_calls) == 1
    assert collector.replay_calls[0] == ("stream-A", 1, collector.replay_calls[0][2])
    assert collector.errors == []


@pytest.mark.anyio
async def test_metrics_on_error_fires_and_reraises(pg_pool, clean_table, monkeypatch):
    collector = _RecordingCollector()
    store = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None, metrics=collector)
    await store.initialize()

    async def boom(stream_id, message):
        raise RuntimeError("db down")

    monkeypatch.setattr(store, "_store_event_impl", boom)

    with pytest.raises(RuntimeError, match="db down"):
        await store.store_event("stream-A", SAMPLE_MSG)

    assert [op for op, _ in collector.errors] == ["store_event"]
    assert collector.store_calls == []


# ── Push-based streaming (parity with test_streaming.py) ──────────────────────


def _stream_msg(i: int) -> JSONRPCRequest:
    return JSONRPCRequest(jsonrpc="2.0", id=str(i), method="tools/call", params={"n": i})


@pytest.mark.anyio
async def test_subscribe_delivers_new_events(pg_pool, clean_table):
    store = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None, enable_streaming=True)
    await store.initialize()

    received: list = []

    async def run():
        async for event_id, message in store.subscribe("stream-A"):
            received.append(message.root.id)
            if len(received) >= 2:
                break

    task = asyncio.create_task(run())
    await asyncio.sleep(0.3)  # let LISTEN register

    await store.store_event("stream-A", _stream_msg(1))
    await store.store_event("stream-A", _stream_msg(2))

    await asyncio.wait_for(task, timeout=5)
    assert received == ["1", "2"]


@pytest.mark.anyio
async def test_subscribe_is_forward_only(pg_pool, clean_table):
    store = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None, enable_streaming=True)
    await store.initialize()

    await store.store_event("stream-A", _stream_msg(0))  # before subscribe

    received: list = []

    async def run():
        async for event_id, message in store.subscribe("stream-A"):
            received.append(message.root.id)
            break

    task = asyncio.create_task(run())
    await asyncio.sleep(0.3)

    await store.store_event("stream-A", _stream_msg(1))

    await asyncio.wait_for(task, timeout=5)
    assert received == ["1"]


@pytest.mark.anyio
async def test_subscribe_cancellation_releases_connection(pg_pool, clean_table):
    store = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None, enable_streaming=True)
    await store.initialize()

    async def run():
        async for _event_id, _message in store.subscribe("stream-A"):
            pass

    task = asyncio.create_task(run())
    await asyncio.sleep(0.3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The released connection is reusable — a normal store still works.
    event_id = await store.store_event("stream-A", _stream_msg(1))
    assert isinstance(event_id, str)


@pytest.mark.anyio
async def test_subscribe_requires_enable_streaming(pg_pool, clean_table):
    store = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None)  # flag defaults False
    await store.initialize()
    with pytest.raises(RuntimeError, match="enable_streaming"):
        async for _ in store.subscribe("stream-A"):
            break


@pytest.mark.anyio
async def test_long_stream_id_notify_channel_within_limit(pg_pool, clean_table):
    # A stream_id long enough to push "mcp_events_<id>" past 63 bytes must still
    # produce a valid (hashed) channel that round-trips through NOTIFY/LISTEN.
    store = PostgresEventStore(pg_pool, table_name=TABLE, ttl=None, enable_streaming=True)
    await store.initialize()

    long_stream = "s" * 200
    channel = store._notify_channel(long_stream)  # pyright: ignore[reportPrivateUsage]
    assert len(channel.encode("utf-8")) <= 63

    received: list = []

    async def run():
        async for _event_id, message in store.subscribe(long_stream):
            received.append(message.root.id)
            break

    task = asyncio.create_task(run())
    await asyncio.sleep(0.3)
    await store.store_event(long_stream, _stream_msg(7))
    await asyncio.wait_for(task, timeout=5)
    assert received == ["7"]
