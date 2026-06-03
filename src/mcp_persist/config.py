"""Construct an event store from environment variables.

:func:`event_store_from_env` reads a small set of ``MCP_PERSIST_*`` variables and
returns the matching backend's :meth:`create` context manager, so a deployment
can pick its store from config without branching on the backend in application
code::

    from mcp_persist import event_store_from_env

    async with event_store_from_env() as store:
        manager = StreamableHTTPSessionManager(app=..., event_store=store)
        ...

Variables:

==================================  ==========================================
``MCP_PERSIST_BACKEND``             ``sqlite`` | ``redis`` | ``postgres`` (required)
``MCP_PERSIST_URL``                 path / URL / DSN for the backend (required)
``MCP_PERSIST_TTL``                 event ttl in seconds (optional integer)
``MCP_PERSIST_TABLE_NAME``          table name (SQLite / Postgres, optional)
``MCP_PERSIST_KEY_PREFIX``          key prefix (Redis, optional)
``MCP_PERSIST_MAX_STREAM_LENGTH``   per-stream cap (Redis, optional integer)
==================================  ==========================================

``MCP_PERSIST_URL`` maps to the first positional argument of each backend's
:meth:`create` (``path`` for SQLite, ``url`` for Redis, ``dsn`` for Postgres), so
it is closed automatically when the returned context manager exits.
"""

from __future__ import annotations

import os
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING

from mcp_persist.postgres import PostgresEventStore
from mcp_persist.redis import RedisEventStore
from mcp_persist.sqlite import SQLiteEventStore

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mcp.server.streamable_http import EventStore

_PREFIX = "MCP_PERSIST_"
_BACKENDS = ("sqlite", "redis", "postgres")


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if not value:
        raise ValueError(f"{name} is required to build an event store from the environment")
    return value


def _optional_int(env: Mapping[str, str], name: str) -> int | None:
    raw = env.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def event_store_from_env(env: Mapping[str, str] | None = None) -> AbstractAsyncContextManager[EventStore]:
    """Build an event store from ``MCP_PERSIST_*`` environment variables.

    Returns the chosen backend's :meth:`create` async context manager (the store
    is opened on ``__aenter__`` and its connection closed on ``__aexit__``); it is
    not entered here. Pass ``env`` to read from a mapping other than
    ``os.environ`` (useful in tests). Raises ``ValueError`` for a missing/invalid
    backend, a missing URL, or a non-integer ``MCP_PERSIST_TTL`` /
    ``MCP_PERSIST_MAX_STREAM_LENGTH``.
    """
    env = os.environ if env is None else env

    backend = _require(env, f"{_PREFIX}BACKEND").strip().lower()
    url = _require(env, f"{_PREFIX}URL")
    ttl = _optional_int(env, f"{_PREFIX}TTL")

    if backend == "sqlite":
        kwargs: dict[str, object] = {"ttl": ttl}
        table = env.get(f"{_PREFIX}TABLE_NAME")
        if table:
            kwargs["table_name"] = table
        return SQLiteEventStore.create(url, **kwargs)  # type: ignore[arg-type]

    if backend == "redis":
        kwargs = {"ttl": ttl}
        prefix = env.get(f"{_PREFIX}KEY_PREFIX")
        if prefix:
            kwargs["key_prefix"] = prefix
        max_stream_length = _optional_int(env, f"{_PREFIX}MAX_STREAM_LENGTH")
        if max_stream_length is not None:
            kwargs["max_stream_length"] = max_stream_length
        return RedisEventStore.create(url, **kwargs)  # type: ignore[arg-type]

    if backend == "postgres":
        kwargs = {"ttl": ttl}
        table = env.get(f"{_PREFIX}TABLE_NAME")
        if table:
            kwargs["table_name"] = table
        return PostgresEventStore.create(url, **kwargs)  # type: ignore[arg-type]

    raise ValueError(f"{_PREFIX}BACKEND must be one of {_BACKENDS}, got {backend!r}")
