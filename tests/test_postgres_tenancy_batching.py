# pyright: reportPrivateUsage=false
# pyright: reportArgumentType=false
"""Postgres tenancy + batching tests (run only with MCP_TEST_POSTGRES_URL).

These exercise the asyncpg positional-parameter numbering of the tenant clause
and the batched ID pre-allocation path, which cannot run against the in-memory
backends. CI sets MCP_TEST_POSTGRES_URL; locally they skip.
"""

from __future__ import annotations

import os

import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import BatchingEventStore, PostgresEventStore

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")
TABLE = "test_tenancy_events"
POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL")

pytestmark = pytest.mark.skipif(POSTGRES_URL is None, reason="set MCP_TEST_POSTGRES_URL to run Postgres tests")


@pytest.fixture
async def pg_pool():
    import asyncpg

    pool = await asyncpg.create_pool(POSTGRES_URL)
    await pool.execute(f"DROP TABLE IF EXISTS {TABLE}")
    try:
        yield pool
    finally:
        await pool.execute(f"DROP TABLE IF EXISTS {TABLE}")
        await pool.close()


async def _replay(store, last_event_id):
    captured: list[EventMessage] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    sid = await store.replay_events_after(last_event_id, cb)
    return sid, captured


@pytest.mark.anyio
async def test_replay_and_list_scoped_by_tenant(pg_pool):
    acme = PostgresEventStore(pg_pool, table_name=TABLE, tenant_id="acme", ttl=None)
    globex = PostgresEventStore(pg_pool, table_name=TABLE, tenant_id="globex", ttl=None)
    await acme.initialize()
    await globex.initialize()

    a_anchor = await acme.store_event("s", None)
    await acme.store_event("s", SAMPLE_MSG)
    await globex.store_event("s", SAMPLE_MSG)

    sid, captured = await _replay(globex, a_anchor)
    assert sid is None and captured == []

    assert {s async for s in acme.list_streams()} == {"s"}
    assert {s async for s in globex.list_streams()} == {"s"}


@pytest.mark.anyio
async def test_purge_and_count_scoped_by_tenant(pg_pool):
    acme = PostgresEventStore(pg_pool, table_name=TABLE, tenant_id="acme", ttl=1)
    globex = PostgresEventStore(pg_pool, table_name=TABLE, tenant_id="globex", ttl=1)
    await acme.initialize()
    await globex.initialize()

    await acme.store_event("s", SAMPLE_MSG)
    await globex.store_event("s", SAMPLE_MSG)
    await pg_pool.execute(f"UPDATE {TABLE} SET created_at = 0")

    assert await acme.count_expired() == 1
    assert await acme.purge_expired(batch_size=10) == 1
    assert await globex.count_expired() == 1


@pytest.mark.anyio
async def test_batching_preallocates_and_persists(pg_pool):
    inner = PostgresEventStore(pg_pool, table_name=TABLE, ttl=3600)
    await inner.initialize()
    batching = BatchingEventStore(inner, flush_max_events=4, flush_max_latency_ms=10_000)

    first = await batching.store_event("s", SAMPLE_MSG)
    for _ in range(3):  # hits flush_max_events -> synchronous flush
        await batching.store_event("s", SAMPLE_MSG)

    _, captured = await _replay(inner, first)
    assert len(captured) == 3
    await batching.aclose()
