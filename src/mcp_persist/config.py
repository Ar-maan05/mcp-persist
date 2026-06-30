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
``MCP_PERSIST_TENANT_ID``           tenant namespace bound at construction (optional)
``MCP_PERSIST_COMPRESSION``         ``gzip`` | ``zstd`` payload codec (optional)
``MCP_PERSIST_ENCRYPTION_KEY``      single base64 AES-256 key for encryption at rest (optional)
``MCP_PERSIST_ENCRYPTION_KEYS``     ``id:b64,id:b64`` key list for rotation (optional)
``MCP_PERSIST_ENCRYPTION_KEY_ID``   active key id when more than one key is listed (optional)
``MCP_PERSIST_BATCH_MAX_EVENTS``    batching wrapper flush size (optional integer)
``MCP_PERSIST_BATCH_MAX_LATENCY_MS`` batching wrapper flush latency (optional integer)

``MCP_PERSIST_URL`` maps to the first positional argument of each backend's
:meth:`create` (``path`` for SQLite, ``url`` for Redis, ``dsn`` for Postgres), so
it is closed automatically when the returned context manager exits.
"""

from __future__ import annotations

import os
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING

from mcp_persist.batching import BatchingEventStore
from mcp_persist.compression import validate_compression
from mcp_persist.encryption import keyring_from_env
from mcp_persist.postgres import PostgresEventStore
from mcp_persist.redis import RedisEventStore
from mcp_persist.sqlite import SQLiteEventStore

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mcp.server.streamable_http import EventStore

    from mcp_persist.retention import RetentionPolicy

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
    tenant_id = env.get(f"{_PREFIX}TENANT_ID") or None
    compression = env.get(f"{_PREFIX}COMPRESSION") or None
    if compression:
        validate_compression(compression)
    keyring = keyring_from_env(env)
    batch_max_events = _optional_int(env, f"{_PREFIX}BATCH_MAX_EVENTS")
    batch_max_latency_ms = env.get(f"{_PREFIX}BATCH_MAX_LATENCY_MS")
    batch_latency = float(batch_max_latency_ms) if batch_max_latency_ms else None

    if backend == "sqlite":
        if batch_max_events is not None or batch_latency is not None:
            raise ValueError(
                "batching is not supported on the sqlite backend: SQLite's own write-behind "
                "(commit_interval / commit_max_pending) already batches the fsync that dominates "
                "its write cost. Drop MCP_PERSIST_BATCH_* or use the redis/postgres backend."
            )
        kwargs: dict[str, object] = {"ttl": ttl}
        table = env.get(f"{_PREFIX}TABLE_NAME")
        if table:
            kwargs["table_name"] = table
        if tenant_id:
            kwargs["tenant_id"] = tenant_id
        if compression:
            kwargs["compression"] = compression
        if keyring is not None:
            kwargs["keyring"] = keyring
        return SQLiteEventStore.create(url, **kwargs)  # type: ignore[arg-type]

    if backend == "redis":
        kwargs = {"ttl": ttl}
        prefix = env.get(f"{_PREFIX}KEY_PREFIX")
        if prefix:
            kwargs["key_prefix"] = prefix
        if tenant_id:
            kwargs["tenant_id"] = tenant_id
        if compression:
            kwargs["compression"] = compression
        if keyring is not None:
            kwargs["keyring"] = keyring
        max_stream_length = _optional_int(env, f"{_PREFIX}MAX_STREAM_LENGTH")
        if max_stream_length is not None:
            kwargs["max_stream_length"] = max_stream_length
        cm = RedisEventStore.create(url, **kwargs)  # type: ignore[arg-type]
        return _maybe_batch(cm, batch_max_events, batch_latency)

    if backend == "postgres":
        kwargs = {"ttl": ttl}
        table = env.get(f"{_PREFIX}TABLE_NAME")
        if table:
            kwargs["table_name"] = table
        if tenant_id:
            kwargs["tenant_id"] = tenant_id
        if compression:
            kwargs["compression"] = compression
        if keyring is not None:
            kwargs["keyring"] = keyring
        cm = PostgresEventStore.create(url, **kwargs)  # type: ignore[arg-type]
        return _maybe_batch(cm, batch_max_events, batch_latency)

    raise ValueError(f"{_PREFIX}BACKEND must be one of {_BACKENDS}, got {backend!r}")


def _maybe_batch(
    inner_cm: AbstractAsyncContextManager[EventStore],
    flush_max_events: int | None,
    flush_max_latency_ms: float | None,
) -> AbstractAsyncContextManager[EventStore]:
    if flush_max_events is None and flush_max_latency_ms is None:
        return inner_cm
    from contextlib import asynccontextmanager

    events = flush_max_events if flush_max_events is not None else 64
    latency = flush_max_latency_ms if flush_max_latency_ms is not None else 50.0

    @asynccontextmanager
    async def wrapped():
        async with inner_cm as store:
            batching = BatchingEventStore(store, flush_max_events=events, flush_max_latency_ms=latency)
            try:
                yield batching  # type: ignore[misc]
            finally:
                await batching.aclose()

    return wrapped()  # type: ignore[return-value]


def retention_policy_from_env(env: Mapping[str, str] | None = None) -> RetentionPolicy | None:
    """Build a RetentionPolicy from MCP_PERSIST_RETENTION_* env vars, or None if unset.

    If neither env var is set, returns None. Raises ValueError if validation
    fails.
    """
    import json
    import os

    from mcp_persist.retention import RetentionPolicy

    target_env = os.environ if env is None else env

    windows_raw = target_env.get("MCP_PERSIST_RETENTION_WINDOWS")
    default_raw = target_env.get("MCP_PERSIST_RETENTION_DEFAULT")

    if windows_raw is None and default_raw is None:
        return None

    windows: dict[str | None, int] = {}
    default_val: int | None = None

    if default_raw is not None:
        try:
            default_val = int(default_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"MCP_PERSIST_RETENTION_DEFAULT must be an integer, got {default_raw!r}") from exc

    if windows_raw is not None:
        try:
            parsed = json.loads(windows_raw)
        except Exception as exc:
            raise ValueError(f"MCP_PERSIST_RETENTION_WINDOWS must be a valid JSON object, got {windows_raw!r}") from exc

        if not isinstance(parsed, dict):
            raise ValueError(f"MCP_PERSIST_RETENTION_WINDOWS must be a JSON object (dict), got {type(parsed).__name__}")

        for k, v in parsed.items():
            key: str | None = None if k == "null" else k

            if k == "__default__":
                try:
                    default_from_windows = int(v)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"__default__ window in JSON must be an integer, got {v!r}") from exc

                if default_val is not None and default_val != default_from_windows:
                    raise ValueError(
                        f"__default__ in JSON ({default_from_windows}) and "
                        f"MCP_PERSIST_RETENTION_DEFAULT ({default_val}) disagree"
                    )
                default_val = default_from_windows
                continue

            try:
                val = int(v)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Retention window for tenant {k!r} must be an integer, got {v!r}") from exc

            windows[key] = val

    return RetentionPolicy(windows=windows, default=default_val)
