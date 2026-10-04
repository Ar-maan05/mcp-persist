# pyright: reportPrivateUsage=false
"""The Redis backends against a real Redis Cluster.

Cluster changes what is allowed: a MULTI/EXEC or a script may only touch keys in
one hash slot, so the event store skips its scripts there and the session
registry keeps its transaction on one key. Those paths are easy to break without
noticing, because a standalone Redis accepts anything, so they run here against
a real cluster-mode server through ``redis.asyncio.cluster.RedisCluster``.

Skipped unless ``MCP_TEST_REDIS_CLUSTER_URL`` points at a cluster-mode Redis
(one node holding every slot is enough). CI starts one.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import anyio
import pytest
from mcp.server.mcpserver import MCPServer
from mcp_types import JSONRPCRequest

from mcp_persist import RedisEventStore, SessionScopedEventStore
from mcp_persist.sessions import session_registry_for

CLUSTER_URL = os.environ.get("MCP_TEST_REDIS_CLUSTER_URL")

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(not CLUSTER_URL, reason="set MCP_TEST_REDIS_CLUSTER_URL to a cluster-mode Redis"),
]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def cluster() -> AsyncIterator[Any]:
    # redis-py has an asyncio cluster client from 4.3, able to pipeline from 4.4;
    # the package allows 4.2. Cluster support is documented as 4.4 and newer.
    cluster_module = pytest.importorskip("redis.asyncio.cluster")
    if not hasattr(cluster_module.RedisCluster, "pipeline"):
        pytest.skip("redis-py's asyncio cluster client cannot pipeline before 4.4")
    assert CLUSTER_URL is not None
    client = cluster_module.RedisCluster.from_url(CLUSTER_URL)
    try:
        yield client
    finally:
        try:
            await client.aclose()
        except AttributeError:  # redis-py < 5.0
            await client.close()


def _msg(n: int) -> JSONRPCRequest:
    return JSONRPCRequest(jsonrpc="2.0", id=str(n), method="tools/list")


def _store(cluster: Any, **kwargs: Any) -> RedisEventStore:
    # A fresh prefix per test keeps tests apart without flushing a shared server.
    return RedisEventStore(cluster, key_prefix=f"ct{uuid.uuid4().hex[:8]}:", ttl=60, **kwargs)


async def _replay(store: Any, last_event_id: str) -> tuple[str | None, list[str]]:
    ids: list[str] = []

    async def collect(event: Any) -> None:
        ids.append(event.event_id)

    return await store.replay_events_after(last_event_id, collect), ids


async def test_events_store_and_replay(cluster: Any) -> None:
    store = _store(cluster, max_stream_length=100)
    anchor = await store.store_event("s", _msg(0))
    after = [await store.store_event("s", _msg(i)) for i in range(1, 6)]

    assert await _replay(store, anchor) == ("s", after)
    assert store._write_script is None  # the slot-spanning scripts are never used on a cluster
    assert store._replay_script is None


async def test_session_registry_round_trip(cluster: Any) -> None:
    registry = session_registry_for(_store(cluster), ttl=60)
    owner = {"client_id": "alice", "issuer": None, "subject": "alice"}
    await registry.register("sess-1", owner=owner, handshake={"protocolVersion": "2025-06-18"})
    await registry.register("sess-1", owner={"client_id": "mallory"})  # a re-register keeps the first owner
    await registry.touch("sess-1")

    record = await registry.get("sess-1")
    assert record is not None and record.owner == owner and not record.terminated
    assert record.handshake == {"protocolVersion": "2025-06-18"}
    assert [r.session_id for r in await registry.list_sessions()] == ["sess-1"]

    await registry.terminate("sess-1")
    ended = await registry.get("sess-1")
    assert ended is not None and ended.terminated
    assert await registry.list_sessions() == []
    assert await registry.purge(older_than=-1) == 1
    assert await registry.get("sess-1") is None


async def test_sessions_cannot_replay_each_others_events(cluster: Any) -> None:
    shared = _store(cluster)
    alice = SessionScopedEventStore(shared, "alice")
    mallory = SessionScopedEventStore(shared, "mallory")
    planted = await mallory.store_event("7", _msg(0))
    first = await alice.store_event("7", _msg(1))
    secret = await alice.store_event("7", _msg(2))

    assert await _replay(mallory, planted) == ("7", [])
    assert await _replay(mallory, first) == (None, [])
    assert await _replay(alice, first) == ("7", [secret])


async def test_a_durable_session_is_adopted_across_workers(cluster: Any) -> None:
    from mcp_persist.session_manager import ResumableSessionManager

    store = _store(cluster)
    registry = session_registry_for(store, ttl=60)
    mcp = MCPServer(name="ClusterServer")

    @mcp.tool()
    def echo(message: str) -> dict[str, str]:
        return {"echo": message}

    def make() -> ResumableSessionManager:
        return ResumableSessionManager(app=mcp._lowlevel_server, event_store=store, registry=registry)

    init = {
        "jsonrpc": "2.0",
        "id": "init-1",
        "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "c", "version": "1"}},
    }
    call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {"name": "echo", "arguments": {"message": "hi"}},
    }
    worker_a, worker_b = make(), make()
    async with worker_a.run(), worker_b.run():
        status, _, session_id = await _post(worker_a, init, None)
        assert status == 200 and session_id
        record = await registry.get(session_id)
        assert record is not None and record.client == "c 1"

        status, body, _ = await _post(worker_b, call, session_id)
        assert status == 200, body
        assert b'"echo":"hi"' in body.replace(b" ", b"")


async def _post(manager: Any, body: dict[str, Any], session_id: str | None) -> tuple[int, bytes, str | None]:
    raw = json.dumps(body).encode()
    headers = [
        (b"accept", b"application/json, text/event-stream"),
        (b"content-type", b"application/json"),
        (b"content-length", str(len(raw)).encode()),
    ]
    if session_id is not None:
        headers += [(b"mcp-session-id", session_id.encode()), (b"mcp-protocol-version", b"2025-06-18")]
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "raw_path": b"/mcp",
        "query_string": b"",
        "headers": headers,
        "server": ("127.0.0.1", 80),
        "client": ("127.0.0.1", 1234),
        "scheme": "http",
        "http_version": "1.1",
    }
    sent = False
    status = 0
    chunks: list[bytes] = []
    new_id: str | None = None

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": raw, "more_body": False}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")

    async def send(message: dict[str, Any]) -> None:
        nonlocal status, new_id
        if message["type"] == "http.response.start":
            status = message["status"]
            for name, value in message["headers"]:
                if name.lower() == b"mcp-session-id":
                    new_id = value.decode()
        elif message["type"] == "http.response.body":
            chunks.append(message.get("body", b""))

    with anyio.fail_after(10):
        await manager.handle_request(scope, receive, send)
    return status, b"".join(chunks), new_id
