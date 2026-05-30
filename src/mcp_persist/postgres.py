"""PostgreSQL-backed EventStore for MCP SSE stream resumability.

Requires the postgres extra:
    pip install "mcp-persist[postgres]"

Quickstart:
    import asyncpg
    from mcp.server.fastmcp import FastMCP
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from mcp_persist import PostgresEventStore

    mcp = FastMCP(name="MyServer")
    pool = await asyncpg.create_pool("postgresql://localhost/mydb")
    store = PostgresEventStore(pool, ttl=3600)
    await store.initialize()

    session_manager = StreamableHTTPSessionManager(
        app=mcp._mcp_server,  # the low-level Server that FastMCP wraps
        event_store=store,
    )

PostgresEventStore targets deployments that already run PostgreSQL and want SSE
resumability that survives restarts — including teams that scale beyond a single
node. It takes an ``asyncpg.Pool`` so concurrent request handlers can store and
replay events without contending on one connection. For a pure single-process
deployment with no external service, ``SQLiteEventStore`` is lighter; for
ephemeral multi-worker fan-out, ``RedisEventStore`` is the better fit.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventMessage,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage
from pydantic import TypeAdapter

logger = logging.getLogger(__name__)

jsonrpc_message_adapter = TypeAdapter(JSONRPCMessage)


class PostgresEventStore(EventStore):
    """EventStore backed by PostgreSQL for durable, scalable SSE resumability.

    Table layout (one row per event):
        event_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY — monotonic EventIds
        stream_id   TEXT  — stream the event belongs to
        payload     TEXT  — serialized JSONRPCMessage ("" for priming events)
        created_at  DOUBLE PRECISION  — unix timestamp, used for ttl expiry

    ``event_id`` is an ``IDENTITY`` column, so IDs are strictly increasing and
    never reused — the same monotonic guarantee as Redis ``INCR`` and SQLite
    ``AUTOINCREMENT``. Replay is an indexed range scan
    (``WHERE stream_id = $1 AND event_id > $2``).

    Args:
        pool:       An ``asyncpg.Pool`` (or any object exposing the same
                    ``execute``/``fetch``/``fetchrow``/``fetchval`` coroutines).
        table_name: Table name. Use different names when multiple MCP servers
                    share one database. Must be a valid SQL identifier.
                    Default: ``"mcp_events"``.
        ttl:        Seconds after which events are considered expired and are
                    skipped on replay (and removed by :meth:`purge_expired`).
                    ``None`` means events never expire — discouraged in
                    production. PostgreSQL has no automatic row expiry, so call
                    :meth:`purge_expired` periodically (e.g. from a background
                    task or ``pg_cron``) to reclaim space.
    """

    def __init__(
        self,
        pool: Any,  # asyncpg.Pool at runtime
        *,
        table_name: str = "mcp_events",
        ttl: int | None = None,
    ) -> None:
        if not table_name.isidentifier():
            raise ValueError(f"table_name must be a valid SQL identifier, got {table_name!r}")

        self._pool = pool
        self._table = table_name
        self._ttl = ttl
        self._initialized = False
        self._init_lock = asyncio.Lock()

        if ttl is None:
            logger.warning(
                "PostgresEventStore created with ttl=None. "
                "Events will accumulate indefinitely. "
                "Set ttl= to a positive number of seconds "
                "(recommended: at least 2x your session_idle_timeout) and call "
                "purge_expired() periodically."
            )

    # Schema

    async def initialize(self) -> None:
        """Create the events table and index if they do not exist.

        Called automatically on first use; safe to call explicitly and
        repeatedly (e.g. at startup). An internal lock serializes the DDL so
        concurrent first ``store_event`` calls on a pool can't both run
        ``CREATE TABLE IF NOT EXISTS`` — concurrent ``CREATE`` can raise a
        duplicate-key error on Postgres system catalogs.
        """
        async with self._init_lock:
            if self._initialized:
                return
            await self._pool.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table} ("
                "event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                "stream_id TEXT NOT NULL, "
                "payload TEXT NOT NULL, "
                "created_at DOUBLE PRECISION NOT NULL)"
            )
            await self._pool.execute(
                f"CREATE INDEX IF NOT EXISTS {self._table}_stream_idx ON {self._table} (stream_id, event_id)"
            )
            self._initialized = True

    # EventStore interface

    async def store_event(
        self,
        stream_id: StreamId,
        message: JSONRPCMessage | None,
    ) -> EventId:
        """Store an event and return its unique, monotonically increasing ID."""
        if not self._initialized:
            await self.initialize()

        if message is None:
            payload = ""
        else:
            payload = message.model_dump_json(by_alias=True, exclude_none=True)

        event_id = await self._pool.fetchval(
            f"INSERT INTO {self._table} (stream_id, payload, created_at) VALUES ($1, $2, $3) RETURNING event_id",
            stream_id,
            payload,
            time.time(),
        )

        return str(event_id)

    async def replay_events_after(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> StreamId | None:
        """Replay all events on the same stream that occurred after last_event_id."""
        if not self._initialized:
            await self.initialize()

        # Last-Event-ID is a client-controlled header; a non-numeric value can't
        # match any stored event, so return None instead of raising on int().
        try:
            anchor_id = int(last_event_id)
        except (TypeError, ValueError):
            return None

        row = await self._pool.fetchrow(
            f"SELECT stream_id FROM {self._table} WHERE event_id = $1",
            anchor_id,
        )

        if row is None:
            return None

        stream_id: StreamId = row["stream_id"]

        if self._ttl is not None:
            rows = await self._pool.fetch(
                f"SELECT event_id, payload FROM {self._table} "
                "WHERE stream_id = $1 AND event_id > $2 AND created_at >= $3 "
                "ORDER BY event_id",
                stream_id,
                anchor_id,
                time.time() - self._ttl,
            )
        else:
            rows = await self._pool.fetch(
                f"SELECT event_id, payload FROM {self._table} WHERE stream_id = $1 AND event_id > $2 ORDER BY event_id",
                stream_id,
                anchor_id,
            )

        for record in rows:
            payload = record["payload"]
            # Priming events (empty payload) are stored but never replayed.
            if not payload:
                continue

            message = jsonrpc_message_adapter.validate_json(payload)
            await send_callback(EventMessage(message=message, event_id=str(record["event_id"])))

        return stream_id

    # Maintenance

    async def purge_expired(self) -> int:
        """Delete events older than ``ttl`` and return the number removed.

        No-op returning ``0`` when ``ttl`` is ``None``. PostgreSQL has no
        automatic row expiry, so schedule this (e.g. from a periodic background
        task or ``pg_cron``) to keep the table from growing without bound.
        (``pg_cron`` is a PostgreSQL extension that runs scheduled jobs inside
        the database itself, so cleanup can run without an external scheduler.)
        """
        if self._ttl is None:
            return 0

        if not self._initialized:
            await self.initialize()

        # asyncpg returns a command tag like "DELETE 5"; the count is the last token.
        result = await self._pool.execute(
            f"DELETE FROM {self._table} WHERE created_at < $1",
            time.time() - self._ttl,
        )
        return int(result.split()[-1])
