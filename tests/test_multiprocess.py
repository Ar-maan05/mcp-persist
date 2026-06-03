"""Multi-process integration test.

A separate OS process writes events to a shared Redis/Postgres backend while this
process concurrently replays them, validating the cross-process resumability that
is the whole reason to choose Redis/Postgres over single-writer SQLite. The
existing stress tests run everything in one process; this one proves a second
process's writes are visible to a replaying reader.

Each test is gated on the relevant backend being configured (``MCP_TEST_REDIS_URL``
/ ``MCP_TEST_POSTGRES_URL``); otherwise it is skipped, like the other integration
tests. All tests are async (anyio/asyncio backend).
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import uuid

import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import PostgresEventStore, RedisEventStore

REDIS_URL = os.environ.get("MCP_TEST_REDIS_URL")
POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL")

_WORKER = os.path.join(os.path.dirname(__file__), "_mp_worker.py")
SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="anchor", method="anchor")
N = 25
STREAM = "mp-stream"


def _spawn_writer(backend: str, url: str, key: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen([sys.executable, _WORKER, backend, url, key, STREAM, str(N)])


async def _await_proc(proc: subprocess.Popen[bytes], timeout: float = 30.0) -> None:
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, lambda: proc.wait(timeout=timeout))
    except subprocess.TimeoutExpired:
        proc.kill()
        raise


async def _drain_until(store, anchor: str, target: int, timeout: float = 30.0) -> set[str]:
    """Replay from ``anchor`` in a loop until ``target`` distinct events are seen."""
    collected: set[str] = set()

    async def cb(event: EventMessage) -> None:
        if event.event_id is not None:
            collected.add(event.event_id)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while len(collected) < target and loop.time() < deadline:
        await store.replay_events_after(anchor, cb)
        if len(collected) >= target:
            break
        await asyncio.sleep(0.05)
    return collected


@pytest.mark.anyio
@pytest.mark.skipif(REDIS_URL is None, reason="set MCP_TEST_REDIS_URL")
async def test_multiprocess_redis_write_visible_to_parent_replay():
    import redis.asyncio as aioredis

    prefix = f"mp:{uuid.uuid4().hex[:8]}:"
    client = aioredis.from_url(REDIS_URL)
    try:
        store = RedisEventStore(client, key_prefix=prefix, ttl=3600)
        anchor = await store.store_event(STREAM, SAMPLE_MSG)

        proc = _spawn_writer("redis", REDIS_URL, prefix)
        try:
            collected = await _drain_until(store, anchor, N)
        finally:
            await _await_proc(proc)

        assert proc.returncode == 0
        assert len(collected) >= N
    finally:
        keys = [k async for k in client.scan_iter(match=f"{prefix}*")]
        if keys:
            await client.delete(*keys)
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


@pytest.mark.anyio
@pytest.mark.skipif(POSTGRES_URL is None, reason="set MCP_TEST_POSTGRES_URL")
async def test_multiprocess_postgres_write_visible_to_parent_replay():
    import asyncpg

    table = f"mp_{uuid.uuid4().hex[:8]}"
    pool = await asyncpg.create_pool(POSTGRES_URL)
    try:
        store = PostgresEventStore(pool, table_name=table, ttl=3600)
        await store.initialize()
        anchor = await store.store_event(STREAM, SAMPLE_MSG)

        proc = _spawn_writer("postgres", POSTGRES_URL, table)
        try:
            collected = await _drain_until(store, anchor, N)
        finally:
            await _await_proc(proc)

        assert proc.returncode == 0
        assert len(collected) >= N
    finally:
        await pool.execute(f'DROP TABLE IF EXISTS "{table}"')
        await pool.close()
