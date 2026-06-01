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
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventMessage,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage
from pydantic import TypeAdapter, ValidationError

from mcp_persist.metrics import NoOpMetricsCollector, safe_call

if TYPE_CHECKING:
    from mcp_persist.metrics import MetricsCollector

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
        metrics:    Optional :class:`~mcp_persist.metrics.MetricsCollector` for
                    timing/count hooks on ``store_event`` and
                    ``replay_events_after``. ``None`` (the default) installs a
                    no-op collector and the store takes a fast path with no
                    measurable overhead.
        enable_streaming:
                    Gates :meth:`subscribe`, which must be opted into. ``False``
                    (the default) makes :meth:`subscribe` raise. Unlike Redis and
                    Postgres, SQLite has no native push, so ``subscribe`` polls
                    the table and ``store_event`` is unaffected by this flag.
    """

    def __init__(
        self,
        conn: Any,  # aiosqlite.Connection at runtime
        *,
        table_name: str = "mcp_events",
        ttl: int | None = None,
        timeout: float | None = None,
        metrics: MetricsCollector | None = None,
        enable_streaming: bool = False,
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
        self._metrics: MetricsCollector = metrics if metrics is not None else NoOpMetricsCollector()
        self._enable_streaming = enable_streaming
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

    # Convenience constructor

    @classmethod
    @asynccontextmanager
    async def create(
        cls,
        path: str,
        *,
        table_name: str = "mcp_events",
        ttl: int | None = None,
        timeout: float | None = None,
        **connect_kwargs: Any,
    ) -> AsyncIterator[SQLiteEventStore]:
        """Open an SQLite connection, initialize, yield a store, and close on exit.

        A convenience async context manager that owns the connection lifecycle so
        callers don't have to open, initialize, and close an ``aiosqlite``
        connection themselves::

            async with SQLiteEventStore.create("events.db", ttl=3600) as store:
                await store.store_event(...)

        ``path`` and any extra ``connect_kwargs`` are passed to
        ``aiosqlite.connect``; ``table_name``, ``ttl``, and ``timeout`` configure
        the store and behave exactly as in :meth:`__init__`. :meth:`initialize`
        is called before the store is yielded. The connection is always closed on
        exit, including when ``initialize`` or the body raises. Pass ``":memory:"``
        for an ephemeral database (note: it is gone once the context exits).

        Requires the ``sqlite`` extra (``pip install "mcp-persist[sqlite]"``); the
        import happens here, not at module import time, so the package loads
        without ``aiosqlite`` installed.
        """
        import aiosqlite

        conn = await aiosqlite.connect(path, **connect_kwargs)
        store = cls(conn, table_name=table_name, ttl=ttl, timeout=timeout)
        try:
            await store.initialize()
            yield store
        finally:
            await conn.close()

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

            # Fast-path check: if table already exists, we can return immediately
            # to avoid exclusive write lock contention on schema DDL.
            try:
                table_parts = self._table.split(".")
                bare = table_parts[-1]
                schema_prefix = f"{table_parts[0]}." if len(table_parts) == 2 else ""
                query = f"SELECT 1 FROM {schema_prefix}sqlite_master WHERE type='table' AND name=?"
                bare_unquoted = bare.strip('"')
                async with self._conn.execute(query, (bare_unquoted,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        if self._timeout is not None:
                            await self._conn.execute(f"PRAGMA busy_timeout = {int(self._timeout * 1000)}")
                        self._initialized = True
                        return
            except Exception:
                pass

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
                # concurrent create can still surface "already exists" or a locking error.
                # The object exists now, so treat it as done rather than crashing startup.
                exc_str = str(exc).lower()
                if "already exists" not in exc_str and "locked" not in exc_str:
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
        if type(self._metrics) is NoOpMetricsCollector:
            return await self._store_event_impl(stream_id, message)
        start = time.monotonic()
        try:
            event_id = await self._store_event_impl(stream_id, message)
        except Exception as exc:
            safe_call(self._metrics.on_error, "store_event", exc)
            raise
        safe_call(self._metrics.on_store_event, stream_id, event_id, (time.monotonic() - start) * 1000.0)
        return event_id

    async def _store_event_impl(
        self,
        stream_id: StreamId,
        message: JSONRPCMessage | None,
    ) -> EventId:
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
        if type(self._metrics) is NoOpMetricsCollector:
            return await self._replay_events_after_impl(last_event_id, send_callback)
        start = time.monotonic()
        count = 0

        async def counting_callback(event: EventMessage) -> None:
            nonlocal count
            count += 1
            await send_callback(event)

        try:
            stream_id = await self._replay_events_after_impl(last_event_id, counting_callback)
        except Exception as exc:
            safe_call(self._metrics.on_error, "replay_events_after", exc)
            raise
        safe_call(self._metrics.on_replay, stream_id, count, (time.monotonic() - start) * 1000.0)
        return stream_id

    async def _replay_events_after_impl(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> StreamId | None:
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

                try:
                    message = jsonrpc_message_adapter.validate_json(payload)
                except ValidationError:
                    # A single corrupt/unparseable payload must not abort the whole
                    # replay: a reconnecting client would otherwise lose every event
                    # on the stream, not just the bad one. Skip it and keep going.
                    logger.warning(
                        "Skipping event %s on stream %s during replay: payload failed JSONRPC validation",
                        event_id_int,
                        stream_id,
                    )
                    continue
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

    # Migration support

    async def list_streams(self) -> AsyncIterator[StreamId]:
        """Yield each distinct stream ID currently stored, in arbitrary order.

        Backs :func:`mcp_persist.migrate` for whole-database migrations.
        """
        if not self._initialized:
            await self.initialize()

        async with self._conn.execute(f"SELECT DISTINCT stream_id FROM {self._table}") as cursor:
            async for (stream_id,) in cursor:
                yield stream_id

    async def _iter_stream_events(self, stream_id: StreamId) -> AsyncIterator[tuple[EventId, JSONRPCMessage | None]]:
        """Yield ``(event_id, message)`` for every stored event on a stream, oldest first.

        Unlike :meth:`replay_events_after`, this enumerates the whole stream from
        the beginning (no anchor) and includes priming events (yielded as a
        ``None`` message), so :func:`mcp_persist.migrate` can copy a stream
        faithfully. Rows are not filtered by ``ttl`` — every stored row is
        yielded; run :meth:`purge_expired` first to drop stale events. A payload
        that fails JSONRPC validation is logged and skipped.
        """
        if not self._initialized:
            await self.initialize()

        async with self._conn.execute(
            f"SELECT event_id, payload FROM {self._table} WHERE stream_id = ? ORDER BY event_id",
            (stream_id,),
        ) as cursor:
            async for event_id_int, payload in cursor:
                event_id = str(event_id_int)
                if not payload:
                    # Priming event: stored with an empty payload, copied as None.
                    yield event_id, None
                    continue

                try:
                    message = jsonrpc_message_adapter.validate_json(payload)
                except ValidationError:
                    logger.warning(
                        "Skipping event %s on stream %s during migration: payload failed JSONRPC validation",
                        event_id,
                        stream_id,
                    )
                    continue

                yield event_id, message

    # Push-based streaming

    async def subscribe(
        self,
        stream_id: StreamId,
        *,
        poll_interval: float = 0.5,
    ) -> AsyncIterator[tuple[EventId, JSONRPCMessage]]:
        """Yield ``(event_id, message)`` for events on a stream in real time.

        Requires ``enable_streaming=True``. SQLite has no native push, so this is
        a polling loop: it records the newest event ID at subscribe time and then
        every ``poll_interval`` seconds queries for events newer than the last
        one seen::

            async for event_id, message in store.subscribe("stream-abc"):
                ...

        **Forward-only.** Only events written *after* the subscription starts are
        delivered (use :meth:`replay_events_after` to catch up on history).
        Latency is bounded by ``poll_interval`` (default 0.5s); lower it for
        snappier delivery at the cost of more queries. Priming events and payloads
        that fail JSONRPC validation are skipped. The generator is cancellable:
        breaking out of the ``async for`` (or cancelling the task) stops polling.
        """
        if not self._enable_streaming:
            raise RuntimeError("subscribe() requires the store to be constructed with enable_streaming=True")

        if poll_interval <= 0:
            raise ValueError(f"poll_interval must be a positive number of seconds, got {poll_interval!r}")

        if not self._initialized:
            await self.initialize()

        # Seed from the current newest event so the subscription is forward-only,
        # even on an empty stream (where MAX returns NULL -> start from 0).
        async with self._conn.execute(
            f"SELECT MAX(event_id) FROM {self._table} WHERE stream_id = ?",
            (stream_id,),
        ) as cursor:
            row = await cursor.fetchone()
        last_seen = row[0] if row is not None and row[0] is not None else 0

        while True:
            async with self._conn.execute(
                f"SELECT event_id, payload FROM {self._table} WHERE stream_id = ? AND event_id > ? ORDER BY event_id",
                (stream_id, last_seen),
            ) as cursor:
                rows = await cursor.fetchall()

            for event_id_int, payload in rows:
                last_seen = event_id_int
                if not payload:
                    # Priming event; not delivered to subscribers.
                    continue

                try:
                    message = jsonrpc_message_adapter.validate_json(payload)
                except ValidationError:
                    logger.warning(
                        "Skipping event %s on stream %s during subscribe: payload failed JSONRPC validation",
                        event_id_int,
                        stream_id,
                    )
                    continue

                yield str(event_id_int), message

            await asyncio.sleep(poll_interval)
