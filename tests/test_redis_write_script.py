# pyright: reportPrivateUsage=false
# pyright: reportArgumentType=false
"""Tests for the single-round-trip Redis write path (``_STORE_EVENT_LUA``).

``store_event`` runs as one ``EVALSHA`` on a scripting-capable standalone Redis
and falls back to the ``INCR`` + pipeline path on Redis Cluster or a server without
scripting. These tests pin both paths to identical, correct, monotonic behavior.

They run against fakeredis. fakeredis only executes Lua when ``lupa`` is installed
(it is, via the dev dependencies), so the scripted path is exercised here; the
fallback path is exercised by forcing it. CI also runs the suite against a real
Redis via ``MCP_TEST_REDIS_URL``, covering the real ``EVALSHA``.
"""

from __future__ import annotations

import os

import fakeredis.aioredis as fakeredis
import pytest
from mcp.types import JSONRPCRequest

from mcp_persist import RedisEventStore

pytestmark = pytest.mark.anyio

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")

REAL_REDIS_URL = os.environ.get("MCP_TEST_REDIS_URL")


@pytest.fixture
async def client():
    if REAL_REDIS_URL:
        import redis.asyncio as real_redis

        c = real_redis.from_url(REAL_REDIS_URL)
        await c.flushdb()
        try:
            yield c
        finally:
            await c.flushdb()
            try:
                await c.aclose()
            except AttributeError:
                await c.close()
    else:
        yield fakeredis.FakeRedis()


async def _replay(store, last_event_id, stream_id):
    events = []

    async def cb(ev):
        events.append(ev)

    resolved = await store.replay_events_after(last_event_id, cb, stream_id=stream_id)
    return resolved, events


# ── The scripted path is taken and is correct ───────────────────────────────


async def test_first_write_settles_script_ok_true(client):
    store = RedisEventStore(client, ttl=60)
    assert store._script_ok is None  # not yet probed
    eid = await store.store_event("s1", SAMPLE_MSG)
    assert eid == "1"
    assert store._script_ok is True
    assert store._write_script is not None


async def test_scripted_path_does_not_call_pipeline(client, monkeypatch):
    # Prove the script path actually carries the write: if it falls through to the
    # pipeline this raises.
    store = RedisEventStore(client, ttl=60)

    async def _boom(*a, **k):
        raise AssertionError("pipeline path should not run when scripting works")

    monkeypatch.setattr(store, "_write_event_pipelined", _boom)
    eid = await store.store_event("s1", SAMPLE_MSG)
    assert eid == "1"


async def test_ids_are_monotonic_under_script(client):
    store = RedisEventStore(client, ttl=60)
    ids = [await store.store_event("s1", SAMPLE_MSG) for _ in range(5)]
    assert ids == ["1", "2", "3", "4", "5"]


async def test_scripted_write_stores_hash_and_index(client):
    store = RedisEventStore(client, ttl=60)
    await store.store_event("s1", SAMPLE_MSG)
    # The event hash and the stream-index entry both exist, exactly as the
    # pipeline path would have written them.
    assert await client.hget("mcp:event:1", "stream_id") in (b"s1", "s1")
    members = await client.zrange("mcp:stream:s1", 0, -1)
    assert [m.decode() if isinstance(m, bytes) else m for m in members] == ["1"]


async def test_scripted_write_applies_ttl(client):
    store = RedisEventStore(client, ttl=120)
    await store.store_event("s1", SAMPLE_MSG)
    assert 0 < await client.ttl("mcp:event:1") <= 120
    assert 0 < await client.ttl("mcp:stream:s1") <= 120


async def test_scripted_write_without_ttl_does_not_expire(client):
    store = RedisEventStore(client, ttl=None)
    await store.store_event("s1", SAMPLE_MSG)
    # -1 == key exists with no expiry.
    assert await client.ttl("mcp:event:1") == -1


async def test_scripted_write_trims_to_max_stream_length(client):
    store = RedisEventStore(client, ttl=60, max_stream_length=3)
    for _ in range(5):
        await store.store_event("s1", SAMPLE_MSG)
    members = await client.zrange("mcp:stream:s1", 0, -1)
    decoded = [m.decode() if isinstance(m, bytes) else m for m in members]
    assert decoded == ["3", "4", "5"]  # only the newest 3 index entries kept


async def test_priming_event_under_script_is_not_replayed(client):
    store = RedisEventStore(client, ttl=60)
    e1 = await store.store_event("s1", SAMPLE_MSG)
    await store.store_event("s1", None)  # priming event: empty payload
    resolved, events = await _replay(store, e1, "s1")
    assert resolved == "s1"
    assert events == []  # the priming event carries no message


# ── The scripted and pipelined paths agree ──────────────────────────────────


async def test_script_and_pipeline_produce_identical_results(client):
    scripted = RedisEventStore(client, ttl=60, key_prefix="sc:")
    pipelined = RedisEventStore(client, ttl=60, key_prefix="pp:")
    pipelined._script_ok = False  # force the fallback path

    for store in (scripted, pipelined):
        e1 = await store.store_event("s1", SAMPLE_MSG)
        await store.store_event("s1", SAMPLE_MSG)
        resolved, events = await _replay(store, e1, "s1")
        assert resolved == "s1"
        assert [e.event_id for e in events] == ["2"]
        assert [e.message.root.method for e in events] == ["tools/list"]

    assert scripted._script_ok is True
    assert pipelined._script_ok is False


# ── Fallback when scripting is unavailable ──────────────────────────────────


async def test_falls_back_to_pipeline_when_scripting_unsupported(client, monkeypatch):
    from redis.exceptions import ResponseError

    class _RaisingScript:
        async def __call__(self, *a, **k):
            raise ResponseError("unknown command 'evalsha'")

    # Simulate a server without scripting: register_script succeeds but invoking
    # the script raises, exactly as a real server lacking EVAL/EVALSHA does.
    monkeypatch.setattr(client, "register_script", lambda src: _RaisingScript())

    store = RedisEventStore(client, ttl=60)
    eid = await store.store_event("s1", SAMPLE_MSG)
    assert eid == "1"
    assert store._script_ok is False  # latched off after the probe

    # Subsequent writes go straight to the pipeline and stay correct/monotonic.
    eid2 = await store.store_event("s1", SAMPLE_MSG)
    assert eid2 == "2"
    resolved, events = await _replay(store, "1", "s1")
    assert resolved == "s1"
    assert [e.event_id for e in events] == ["2"]


async def test_genuine_error_after_script_settled_propagates(client):
    from redis.exceptions import ResponseError

    store = RedisEventStore(client, ttl=60)
    await store.store_event("s1", SAMPLE_MSG)  # settles _script_ok = True
    assert store._script_ok is True

    class _BoomScript:
        async def __call__(self, *a, **k):
            raise ResponseError("OOM command not allowed")

    # Once scripting has worked, a later script error is a real failure, not a
    # "scripting unsupported" probe result, so it must not be swallowed. Swap in a
    # cached script object that raises (the store reuses _write_script as-is).
    store._write_script = _BoomScript()
    with pytest.raises(ResponseError, match="OOM"):
        await store.store_event("s1", SAMPLE_MSG)


# ── Cluster detection ───────────────────────────────────────────────────────


def test_scripting_possible_true_for_standalone():
    store = RedisEventStore(fakeredis.FakeRedis(), ttl=60)
    assert store._scripting_possible() is True


def test_scripting_possible_false_for_cluster_client():
    class RedisCluster:  # name mimics redis.asyncio.cluster.RedisCluster
        pass

    store = RedisEventStore(RedisCluster(), ttl=60)
    assert store._scripting_possible() is False


async def test_cluster_client_never_registers_script(monkeypatch):
    # A cluster-shaped client must take the pipeline path without ever trying to
    # register or run the slot-spanning script.
    real = fakeredis.FakeRedis()

    class FakeClusterClient:
        # Delegate everything to a standalone fake so the pipeline path works,
        # but present a cluster class name so _scripting_possible() returns False.
        def __getattr__(self, name):
            return getattr(real, name)

    FakeClusterClient.__name__ = "RedisCluster"
    store = RedisEventStore(FakeClusterClient(), ttl=60)

    def _no_register(src):
        raise AssertionError("cluster client must not register the write script")

    monkeypatch.setattr(real, "register_script", _no_register)
    eid = await store.store_event("s1", SAMPLE_MSG)
    assert eid == "1"
    assert store._write_script is None
    assert store._script_ok is None  # never probed
