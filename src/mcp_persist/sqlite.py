"""SQLite-backed EventStore for MCP SSE stream resumability.

Requires the sqlite extra:
    pip install "mcp-persist[sqlite]"

Quickstart:
    import aiosqlite
    from mcp.server.fastmcp import FastMCP
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from mcp_persist import SQLiteEventStore

    mcp = FastMCP(name="MyServer")
    conn = await aiosqlite.connect("mcp_events.db")
    store = SQLiteEventStore(conn, ttl=3600)
    await store.initialize()

    session_manager = StreamableHTTPSessionManager(
        app=mcp._mcp_server,  # the low-level Server that FastMCP wraps
        event_store=store,
    )

Unlike RedisEventStore (built for multi-process / multi-worker deployments),
SQLiteEventStore targets a single process that needs SSE resumability to
survive restarts or redeploys without running an external service.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
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

IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


class SQLiteEventStore(EventStore):
    """EventStore backed by SQLite for single-node durability across restarts.

    Table layout (one row per event):
        event_id    INTEGER PRIMARY KEY AUTOINCREMENT — monotonic EventIds
        stream_id   TEXT  — stream the event belongs to
        payload     TEXT  — serialized JSONRPCMessage ("" for priming events)
        created_at  REAL  — unix timestamp, used for ttl expiry

    ``event_id`` uses ``AUTOINCREMENT`` so IDs are strictly increasing and never
    reused, giving the same monotonic guarantee as Redis ``INCR``. Replay is an
    indexed range scan (``WHERE stream_id = ? AND event_id > ?``).

    Args:
        conn:       An open ``aiosqlite.Connection`` **dedicated to this store**.
                    The store calls ``commit()`` on it, which flushes the whole
                    connection's transaction; sharing it with other code that
                    keeps an open transaction would commit that code's pending
                    writes too. Give the store its own connection.
        table_name: Table name, optionally schema-qualified (``"schema.table"``)
                    to target an attached database. Use different names when
                    multiple MCP servers share one database file. Each
                    dot-separated part must be a valid SQL identifier.
                    Default: ``"mcp_events"``.
        ttl:        Seconds after which events are considered expired and are
                    skipped on replay (and removed by :meth:`purge_expired`).
                    ``None`` means events never expire — discouraged in
                    production. SQLite has no automatic expiry, so call
                    :meth:`purge_expired` periodically to reclaim space.
        timeout:    Optional busy timeout in seconds. When set, SQLite waits up
                    to this long for a lock before raising instead of failing
                    immediately (applied via ``PRAGMA busy_timeout``). ``None``
                    leaves SQLite's default in place.
    """

    def __init__(
        self,
        conn: Any,  # aiosqlite.Connection at runtime
        *,
        table_name: str = "mcp_events",
        ttl: int | None = None,
        timeout: float | None = None,
    ) -> None:
        parts = table_name.split(".")
        if len(parts) > 2 or not all(part and IDENTIFIER_RE.match(part) for part in parts):
            raise ValueError(f"table_name must be a valid SQL identifier or 'schema.table', got {table_name!r}")

        self._conn = conn
        quoted_parts = [f'"{part}"' for part in parts]
        self._table = ".".join(quoted_parts)
        # In SQLite a schema-qualified table lives in an attached database, and
        # its index name must carry the same schema while the ON clause names
        # the bare table. With no schema both collapse to the plain name.
        bare = parts[-1]
        schema_prefix = f'"{parts[0]}".' if len(parts) == 2 else ""
        self._index_target = f'"{bare}"'
        self._stream_index = f'{schema_prefix}"{bare}_stream_idx"'
        self._created_index = f'{schema_prefix}"{bare}_created_idx"'
        self._ttl = ttl
        self._timeout = timeout
        self._initialized = False
        self._init_lock = asyncio.Lock()

        if ttl is None:
            logger.warning(
                "SQLiteEventStore created with ttl=None. "
                "Events will accumulate indefinitely. "
                "Set ttl= to a positive number of seconds "
                "(recommended: at least 2x your session_idle_timeout) and call "
                "purge_expired() periodically."
            )

    # Schema

    async def initialize(self) -> None:
        """Create the events table and indexes if they do not exist.

        Called automatically on first use; safe to call explicitly and
        repeatedly (e.g. at startup). Creates a ``(stream_id, event_id)`` index
        for replay range scans and a ``created_at`` index so
        :meth:`purge_expired` deletes by age without a full table scan.
        """
        async with self._init_lock:
            if self._initialized:
                return

            await self._conn.execute("PRAGMA journal_mode=WAL")
            if self._timeout is not None:
                await self._conn.execute(f"PRAGMA busy_timeout = {int(self._timeout * 1000)}")

            try:
                await self._conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {self._table} ("
                    "event_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "stream_id TEXT NOT NULL, "
                    "payload TEXT NOT NULL, "
                    "created_at REAL NOT NULL)"
                )
                await self._conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {self._stream_index} ON {self._index_target} (stream_id, event_id)"
                )
                await self._conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {self._created_index} ON {self._index_target} (created_at)"
                )
                await self._conn.commit()
            except sqlite3.OperationalError as exc:
                # IF NOT EXISTS handles the normal case; another connection winning a
                # concurrent create can still surface "already exists". The object
                # exists now, so treat it as done rather than crashing startup.
                if "already exists" not in str(exc).lower():
                    raise
                logger.debug("Tolerating concurrent DDL race on %s: %s", self._table, exc)

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

        async with self._conn.execute(
            f"INSERT INTO {self._table} (stream_id, payload, created_at) VALUES (?, ?, ?)",
            (stream_id, payload, time.time()),
        ) as cursor:
            await self._conn.commit()
            return str(cursor.lastrowid)

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

        async with self._conn.execute(
            f"SELECT stream_id FROM {self._table} WHERE event_id = ?",
            (anchor_id,),
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            return None

        stream_id: StreamId = row[0]

        if self._ttl is not None:
            query = (
                f"SELECT event_id, payload FROM {self._table} "
                "WHERE stream_id = ? AND event_id > ? AND created_at >= ? "
                "ORDER BY event_id"
            )
            params: tuple[Any, ...] = (
                stream_id,
                anchor_id,
                time.time() - self._ttl,
            )
        else:
            query = (
                f"SELECT event_id, payload FROM {self._table} WHERE stream_id = ? AND event_id > ? ORDER BY event_id"
            )
            params = (stream_id, anchor_id)

        # Stream rows one at a time rather than fetchall()-ing the whole backlog
        # into memory: a client resuming from a long-idle Last-Event-ID could
        # otherwise pull hundreds of thousands of rows into RAM at once.
        async with self._conn.execute(query, params) as cursor:
            async for event_id_int, payload in cursor:
                # Priming events (empty payload) are stored but never replayed.
                if not payload:
                    continue

                message = jsonrpc_message_adapter.validate_json(payload)
                await send_callback(EventMessage(message=message, event_id=str(event_id_int)))

        return stream_id

    # Maintenance

    async def purge_expired(self) -> int:
        """Delete events older than ``ttl`` and return the number removed.

        No-op returning ``0`` when ``ttl`` is ``None``. SQLite has no automatic
        key expiry, so schedule this (e.g. from a periodic background task) to
        keep the database from growing without bound.
        """
        if self._ttl is None:
            return 0

        if not self._initialized:
            await self.initialize()

        async with self._conn.execute(
            f"DELETE FROM {self._table} WHERE created_at < ?",
            (time.time() - self._ttl,),
        ) as cursor:
            await self._conn.commit()
            return cursor.rowcount
