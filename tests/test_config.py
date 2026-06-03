"""Tests for event_store_from_env.

The SQLite path is exercised end-to-end (no external service needed). The Redis
and Postgres paths are validated up to the returned context manager — entering it
would require a live server — since the connection is only opened on
``__aenter__``.

All store-using tests are async (anyio/asyncio backend).
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager

import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import event_store_from_env

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


async def _roundtrip(store) -> list[str | None]:
    anchor = await store.store_event("s", SAMPLE_MSG)
    expected = await store.store_event("s", SAMPLE_MSG)
    captured: list[str | None] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event.event_id)

    await store.replay_events_after(anchor, cb)
    assert captured == [expected]
    return captured


@pytest.mark.anyio
async def test_from_env_sqlite_roundtrip():
    env = {"MCP_PERSIST_BACKEND": "sqlite", "MCP_PERSIST_URL": ":memory:", "MCP_PERSIST_TTL": "3600"}
    cm = event_store_from_env(env)
    assert isinstance(cm, AbstractAsyncContextManager)
    async with cm as store:
        await _roundtrip(store)


@pytest.mark.anyio
async def test_from_env_sqlite_custom_table():
    env = {"MCP_PERSIST_BACKEND": "sqlite", "MCP_PERSIST_URL": ":memory:", "MCP_PERSIST_TABLE_NAME": "custom_tbl"}
    async with event_store_from_env(env) as store:
        await _roundtrip(store)


def test_from_env_redis_returns_context_manager():
    env = {
        "MCP_PERSIST_BACKEND": "redis",
        "MCP_PERSIST_URL": "redis://localhost:6379/0",
        "MCP_PERSIST_KEY_PREFIX": "x:",
        "MCP_PERSIST_MAX_STREAM_LENGTH": "100",
    }
    assert isinstance(event_store_from_env(env), AbstractAsyncContextManager)


def test_from_env_postgres_returns_context_manager():
    env = {"MCP_PERSIST_BACKEND": "postgres", "MCP_PERSIST_URL": "postgresql://localhost/db"}
    assert isinstance(event_store_from_env(env), AbstractAsyncContextManager)


def test_from_env_case_insensitive_backend():
    env = {"MCP_PERSIST_BACKEND": "SQLite", "MCP_PERSIST_URL": ":memory:"}
    assert isinstance(event_store_from_env(env), AbstractAsyncContextManager)


def test_from_env_missing_backend_raises():
    with pytest.raises(ValueError):
        event_store_from_env({"MCP_PERSIST_URL": ":memory:"})


def test_from_env_missing_url_raises():
    with pytest.raises(ValueError):
        event_store_from_env({"MCP_PERSIST_BACKEND": "sqlite"})


def test_from_env_invalid_backend_raises():
    with pytest.raises(ValueError):
        event_store_from_env({"MCP_PERSIST_BACKEND": "mongo", "MCP_PERSIST_URL": "x"})


def test_from_env_invalid_ttl_raises():
    with pytest.raises(ValueError):
        event_store_from_env(
            {"MCP_PERSIST_BACKEND": "sqlite", "MCP_PERSIST_URL": ":memory:", "MCP_PERSIST_TTL": "not-an-int"}
        )
