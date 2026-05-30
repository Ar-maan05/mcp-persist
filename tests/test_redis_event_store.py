# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Tests for RedisEventStore.

By default these run against fakeredis — no external Redis server required.
Set MCP_TEST_REDIS_URL (e.g. redis://localhost:6379/0) to run the entire suite
against a real Redis instead; the target database is flushed around every test,
so point it at a throwaway database. CI runs both: fakeredis for the unit pass
and a real Redis service container via MCP_TEST_REDIS_URL.

All tests are async (anyio/asyncio backend).
"""

from __future__ import annotations

import asyncio
import logging
import os

import fakeredis.aioredis as fakeredis
import pytest
from mcp.server.streamable_http import EventId, EventMessage, StreamId
from mcp.types import JSONRPCRequest

from mcp_persist import RedisEventStore

# ── Helpers ───────────────────────────────────────────────────────────────────

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")

REAL_REDIS_URL = os.environ.get("MCP_TEST_REDIS_URL")


@pytest.fixture
async def redis_client():
    if REAL_REDIS_URL:
        import redis.asyncio as real_redis

        # decode_responses defaults to False -> bytes, matching the assertions
        # in this suite (e.g. hget(...) == b"...").
        client = real_redis.from_url(REAL_REDIS_URL)

        # Guard against pointing at real data: this suite FLUSHDBs around every
        # test, so refuse to run unless the target database is already empty.
        # Checked before any flush, so a non-empty DB is left untouched.
        existing = await client.dbsize()
        if existing:
            try:
                await client.aclose()
            except AttributeError:
                await client.close()
            raise RuntimeError(
                f"MCP_TEST_REDIS_URL points at a database holding {existing} key(s). "
                "This suite calls FLUSHDB around every test; refusing to wipe a "
                "non-empty database. Point it at an empty, throwaway DB."
            )

        # Flush before AND after each test: counter assertions (id == "1") and
        # key-listing assertions require an empty database, and the suite runs
        # serially so a shared real Redis is safe to wipe between tests.
        await client.flushdb()
    else:
        client = fakeredis.FakeRedis()

    try:
        yield client
    finally:
        if REAL_REDIS_URL:
            await client.flushdb()
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


@pytest.fixture
def store(redis_client, recwarn):
    return RedisEventStore(redis_client, key_prefix="test:", ttl=None)


@pytest.fixture
def store_with_ttl(redis_client):
    return RedisEventStore(redis_client, key_prefix="test:", ttl=60)


# ── Shared helper ─────────────────────────────────────────────────────────────


async def collect_events(
    store: RedisEventStore,
    last_event_id: EventId,
) -> tuple[list[EventMessage], StreamId | None]:
    captured: list[EventMessage] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    stream_id = await store.replay_events_after(last_event_id, cb)
    return captured, stream_id


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
async def test_store_priming_event_writes_empty_payload(store, redis_client):
    event_id = await store.store_event("stream-A", None)

    raw = await redis_client.hget(f"test:event:{event_id}", "payload")
    assert raw == b""


@pytest.mark.anyio
async def test_store_event_writes_stream_id_to_hash(store, redis_client):
    event_id = await store.store_event("my-stream", SAMPLE_MSG)

    raw_stream = await redis_client.hget(f"test:event:{event_id}", "stream_id")
    assert raw_stream == b"my-stream"


@pytest.mark.anyio
async def test_store_event_adds_to_sorted_set(store, redis_client):
    id1 = await store.store_event("stream-A", SAMPLE_MSG)
    id2 = await store.store_event("stream-A", SAMPLE_MSG)

    members = await redis_client.zrange("test:stream:stream-A", 0, -1)
    decoded = [m.decode() for m in members]
    assert id1 in decoded
    assert id2 in decoded
    assert decoded.index(id1) < decoded.index(id2)


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
async def test_replay_skips_expired_event_payloads(store, redis_client):
    anchor = await store.store_event("stream-A", SAMPLE_MSG)
    id2 = await store.store_event("stream-A", SAMPLE_MSG)
    id3 = await store.store_event("stream-A", SAMPLE_MSG)

    await redis_client.delete(f"test:event:{id2}")

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
async def test_event_key_has_ttl_when_configured(store_with_ttl, redis_client):
    event_id = await store_with_ttl.store_event("stream-A", SAMPLE_MSG)

    ttl = await redis_client.ttl(f"test:event:{event_id}")
    assert 0 < ttl <= 60


@pytest.mark.anyio
async def test_stream_key_has_ttl_when_configured(store_with_ttl, redis_client):
    await store_with_ttl.store_event("stream-A", SAMPLE_MSG)

    ttl = await redis_client.ttl("test:stream:stream-A")
    assert 0 < ttl <= 60


@pytest.mark.anyio
async def test_counter_key_never_expires_even_with_ttl(store_with_ttl, redis_client):
    # The counter is the monotonic-ID source and must outlive the events it
    # numbers. If it expired alongside them, the next event ID would restart
    # from 1 after an idle gap longer than ttl, breaking monotonicity — so it is
    # never given a TTL, even when one is configured for events.
    await store_with_ttl.store_event("stream-A", SAMPLE_MSG)

    ttl = await redis_client.ttl("test:counter")
    assert ttl == -1  # -1 = key exists with no expiry set


@pytest.mark.anyio
async def test_no_ttl_on_keys_when_not_configured(store, redis_client):
    event_id = await store.store_event("stream-A", SAMPLE_MSG)

    event_ttl = await redis_client.ttl(f"test:event:{event_id}")
    stream_ttl = await redis_client.ttl("test:stream:stream-A")
    counter_ttl = await redis_client.ttl("test:counter")

    assert event_ttl == -1
    assert stream_ttl == -1
    assert counter_ttl == -1


# ─────────────────────────────────────────────────────────────────────────────
# Key prefix test
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_custom_key_prefix_isolates_two_stores(redis_client, recwarn):
    store_a = RedisEventStore(redis_client, key_prefix="server-a:", ttl=None)
    store_b = RedisEventStore(redis_client, key_prefix="server-b:", ttl=None)

    id_a = await store_a.store_event("stream-1", SAMPLE_MSG)
    id_b = await store_b.store_event("stream-1", SAMPLE_MSG)

    assert id_a == "1"
    assert id_b == "1"

    a_keys = [k.decode() for k in await redis_client.keys("server-a:*")]
    b_keys = [k.decode() for k in await redis_client.keys("server-b:*")]

    assert all("server-b:" not in k for k in a_keys)
    assert all("server-a:" not in k for k in b_keys)

    events_a, stream_id_a = await collect_events(store_a, id_a)
    assert stream_id_a == "stream-1"
    assert events_a == []


# ─────────────────────────────────────────────────────────────────────────────
# Warning / logging tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_no_ttl_emits_log_warning(redis_client, caplog):
    with caplog.at_level(logging.WARNING, logger="mcp_persist.redis"):
        RedisEventStore(redis_client, ttl=None)

    assert any("ttl=None" in record.message for record in caplog.records)


@pytest.mark.anyio
async def test_with_ttl_no_warning_emitted(redis_client, caplog):
    with caplog.at_level(logging.WARNING, logger="mcp_persist.redis"):
        RedisEventStore(redis_client, ttl=3600)

    assert not any("ttl" in record.message.lower() for record in caplog.records)


# ─────────────────────────────────────────────────────────────────────────────
# decode_responses support
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_replay_works_with_decode_responses_true():
    # A client created with decode_responses=True returns str, not bytes. The
    # store must handle both — previously replay crashed on str.decode().
    if REAL_REDIS_URL:
        import redis.asyncio as real_redis

        client = real_redis.from_url(REAL_REDIS_URL, decode_responses=True)
    else:
        client = fakeredis.FakeRedis(decode_responses=True)

    try:
        store = RedisEventStore(client, key_prefix="decode-test:", ttl=60)
        anchor = await store.store_event("stream-A", SAMPLE_MSG)
        id2 = await store.store_event("stream-A", SAMPLE_MSG)

        events, stream_id = await collect_events(store, anchor)

        assert stream_id == "stream-A"
        assert [e.event_id for e in events] == [id2]
    finally:
        keys = await client.keys("decode-test:*")
        if keys:
            await client.delete(*keys)
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Sorted-set memory bounds (lazy prune + max_stream_length)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_replay_prunes_stale_zset_members(store, redis_client):
    # An event whose payload hash has expired but whose id still lingers in the
    # stream index must be dropped from the index on replay, so the sorted set
    # can't grow without bound on a long-lived stream.
    anchor = await store.store_event("stream-A", SAMPLE_MSG)
    id2 = await store.store_event("stream-A", SAMPLE_MSG)
    id3 = await store.store_event("stream-A", SAMPLE_MSG)

    await redis_client.delete(f"test:event:{id2}")
    before = [m.decode() for m in await redis_client.zrange("test:stream:stream-A", 0, -1)]
    assert id2 in before

    events, _ = await collect_events(store, anchor)
    assert [e.event_id for e in events] == [id3]

    after = [m.decode() for m in await redis_client.zrange("test:stream:stream-A", 0, -1)]
    assert id2 not in after
    assert id3 in after


@pytest.mark.anyio
async def test_max_stream_length_caps_sorted_set(redis_client, recwarn):
    store = RedisEventStore(redis_client, key_prefix="cap:", ttl=None, max_stream_length=5)

    ids = [await store.store_event("s", SAMPLE_MSG) for _ in range(10)]

    members = [m.decode() for m in await redis_client.zrange("cap:stream:s", 0, -1)]
    # Only the newest 5 ids are retained, in ascending (event-id) order.
    assert members == ids[-5:]


def test_max_stream_length_must_be_positive():
    with pytest.raises(ValueError):
        RedisEventStore(object(), max_stream_length=0)
