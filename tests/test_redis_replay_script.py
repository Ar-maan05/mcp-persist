# pyright: reportPrivateUsage=false
# pyright: reportArgumentType=false
"""Tests for the chunked Redis replay path (``_REPLAY_CHUNK_LUA``).

``replay_events_after`` reads a stream in chunks, one script call per chunk, on a
scripting-capable standalone Redis, and falls back to a stream-index read plus a
pipelined fetch of every payload on Redis Cluster or a server without scripting.
These tests pin both paths to the same results.

Like the write-path tests they run against fakeredis (which executes Lua with
``lupa``) and against a real Redis in CI via ``MCP_TEST_REDIS_URL``.
"""

from __future__ import annotations

import os

import fakeredis.aioredis as fakeredis
import pytest
from mcp_types import JSONRPCRequest

import mcp_persist.redis as redis_module
from mcp_persist import RedisEventStore

pytestmark = pytest.mark.anyio

REAL_REDIS_URL = os.environ.get("MCP_TEST_REDIS_URL")


def _msg(n: int) -> JSONRPCRequest:
    return JSONRPCRequest(jsonrpc="2.0", id=str(n), method="tools/list")


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
                await c.close(close_connection_pool=True)
    else:
        yield fakeredis.FakeRedis()


async def _replay(store: RedisEventStore, last_event_id: str) -> tuple[str | None, list[str]]:
    ids: list[str] = []

    async def cb(ev):
        ids.append(ev.event_id)

    resolved = await store.replay_events_after(last_event_id, cb)
    return resolved, ids


async def _fill(store: RedisEventStore, stream: str, count: int) -> tuple[str, list[str]]:
    anchor = await store.store_event(stream, _msg(0))
    after = [await store.store_event(stream, _msg(i)) for i in range(1, count + 1)]
    return anchor, after


@pytest.mark.parametrize("count", [0, 2, 3, 6, 7])
async def test_replay_reads_across_chunk_boundaries(client, monkeypatch, count):
    # Short chunks, so a few events span several script calls, including a stream
    # that ends exactly on a chunk boundary.
    monkeypatch.setattr(redis_module, "_REPLAY_CHUNK", 3)
    store = RedisEventStore(client, ttl=60, key_prefix="chunk:")
    anchor, after = await _fill(store, "s", count)

    resolved, ids = await _replay(store, anchor)

    assert resolved == "s"
    assert ids == after
    assert store._replay_script is not None  # the scripted path was taken


@pytest.mark.parametrize("scripted", [True, False], ids=["scripted", "pipelined"])
async def test_a_missing_payload_is_skipped_and_pruned(client, monkeypatch, scripted):
    monkeypatch.setattr(redis_module, "_REPLAY_CHUNK", 2)
    store = RedisEventStore(client, ttl=60, key_prefix=f"gap{scripted}:")
    anchor, after = await _fill(store, "s", 5)
    if not scripted:
        store._script_ok = False
    await client.delete(store._event_key(after[2]))  # its hash expired; its index entry did not

    resolved, ids = await _replay(store, anchor)

    assert resolved == "s"
    assert ids == after[:2] + after[3:]
    remaining = [m.decode() if isinstance(m, bytes) else m for m in await client.zrange(store._stream_key("s"), 0, -1)]
    assert after[2] not in remaining


async def test_replay_falls_back_when_scripting_is_unsupported(client, monkeypatch):
    from redis.exceptions import ResponseError

    writer = RedisEventStore(client, ttl=60, key_prefix="nolua:")
    anchor, after = await _fill(writer, "s", 4)

    class _RaisingScript:
        async def __call__(self, *a, **k):
            raise ResponseError("unknown command 'evalsha'")

    monkeypatch.setattr(client, "register_script", lambda src: _RaisingScript())
    reader = RedisEventStore(client, ttl=60, key_prefix="nolua:")  # never probed

    resolved, ids = await _replay(reader, anchor)

    assert resolved == "s"
    assert ids == after
    assert reader._script_ok is False


async def test_a_genuine_error_after_scripting_settled_propagates(client):
    from redis.exceptions import ResponseError

    store = RedisEventStore(client, ttl=60, key_prefix="boom:")
    anchor, _ = await _fill(store, "s", 2)
    assert store._script_ok is True

    class _BoomScript:
        async def __call__(self, *a, **k):
            raise ResponseError("OOM command not allowed")

    store._replay_script = _BoomScript()
    with pytest.raises(ResponseError, match="OOM"):
        await _replay(store, anchor)


async def test_cluster_client_replays_without_the_script(monkeypatch):
    real = fakeredis.FakeRedis()

    class FakeClusterClient:
        def __getattr__(self, name):
            return getattr(real, name)

    FakeClusterClient.__name__ = "RedisCluster"
    store = RedisEventStore(FakeClusterClient(), ttl=60)

    def _no_register(src):
        raise AssertionError("a cluster client must not register a script")

    monkeypatch.setattr(real, "register_script", _no_register)
    anchor, after = await _fill(store, "s", 3)

    resolved, ids = await _replay(store, anchor)

    assert (resolved, ids) == ("s", after)
    assert store._replay_script is None
