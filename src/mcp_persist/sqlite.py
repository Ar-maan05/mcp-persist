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
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventMessage,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage
from pydantic import TypeAdapter

from mcp_persist.compression import compress_payload, decompress_payload, validate_compression
from mcp_persist.metrics import NoOpMetricsCollector, safe_call
from mcp_persist.stored import StoredEvent

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
        compression:
                    Optional payload codec. ``"gzip"`` gzip-compresses event
                    payloads above ``compress_min_bytes`` before storing them;
                    ``None`` (the default) stores them as-is. Decompression on
                    read is automatic and independent of this setting — a store
                    reads compressed payloads written by another store even with
                    compression off, so the option is safe to roll out
                    incrementally and across :func:`mcp_persist.migrate`.
        compress_min_bytes:
                    Only compress payloads whose serialized size is at least this
                    many bytes (default ``1024``). Smaller payloads are stored
                    plain, since base64 overhead would outweigh the saving.
                    Ignored when ``compression`` is ``None``.
        commit_interval:
                    Optional write-behind interval in seconds. When set (must be
                    ``> 0``), ``store_event`` no longer commits on every call;
                    instead a background task commits all buffered inserts every
                    ``commit_interval`` seconds, trading durability for throughput
                    (one fsync per interval instead of one per event). Buffered
                    events stay fully visible to replay/subscribe on this store
                    immediately (they live in SQLite's open transaction), but a
                    process crash loses up to one interval of uncommitted events.
                    ``None`` (the default) keeps durable commit-per-event.
                    **With write-behind on you must close the store** — via
                    :meth:`aclose`, ``async with``, or :meth:`create` — so the
                    final interval is flushed on shutdown.
        commit_max_pending:
                    Optional cap on buffered (uncommitted) events. When set (must
                    be ``>= 1``), ``store_event`` commits inline once this many
                    events are pending, bounding both the crash-loss window and the
                    size of the open transaction under bursts. Combine with
                    ``commit_interval`` (whichever limit is hit first commits), use
                    alone for pure count-based group commit (the tail below the cap
                    is flushed on close), or leave ``None`` (the default).
    """

    def __init__(
        self,
        conn: Any,  # aiosqlite.Connection at runtime
        *,
        table_name: str = "mcp_events",
        tenant_id: str | None = None,
        ttl: int | None = None,
        timeout: float | None = None,
        metrics: MetricsCollector | None = None,
        enable_streaming: bool = False,
        compression: str | None = None,
        compress_min_bytes: int = 1024,
        commit_interval: float | None = None,
        commit_max_pending: int | None = None,
    ) -> None:
        parts = table_name.split(".")
        if len(parts) > 2 or not all(part and IDENTIFIER_RE.match(part) for part in parts):
            raise ValueError(f"table_name must be a valid SQL identifier or 'schema.table', got {table_name!r}")
        validate_compression(compression)
        if compress_min_bytes < 0:
            raise ValueError(f"compress_min_bytes must be a non-negative integer, got {compress_min_bytes!r}")
        if commit_interval is not None and commit_interval <= 0:
            raise ValueError(f"commit_interval must be a positive number of seconds or None, got {commit_interval!r}")
        if commit_max_pending is not None and commit_max_pending < 1:
            raise ValueError(f"commit_max_pending must be a positive integer or None, got {commit_max_pending!r}")

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
        self._tenant_id = tenant_id
        self._tenant_column_ready = False
        self._timeout = timeout
        self._metrics: MetricsCollector = metrics if metrics is not None else NoOpMetricsCollector()
        self._enable_streaming = enable_streaming
        self._compression = compression
        self._compress_min_bytes = compress_min_bytes
        self._commit_interval = commit_interval
        self._commit_max_pending = commit_max_pending
        self._pending = 0
        self._flush_task: asyncio.Task[None] | None = None
        self._commit_lock = asyncio.Lock()
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
        tenant_id: str | None = None,
        ttl: int | None = None,
        timeout: float | None = None,
        commit_interval: float | None = None,
        commit_max_pending: int | None = None,
        **connect_kwargs: Any,
    ) -> AsyncIterator[SQLiteEventStore]:
        """Open an SQLite connection, initialize, yield a store, and close on exit.

        A convenience async context manager that owns the connection lifecycle so
        callers don't have to open, initialize, and close an ``aiosqlite``
        connection themselves::

            async with SQLiteEventStore.create("events.db", ttl=3600) as store:
                await store.store_event(...)

        ``path`` and any extra ``connect_kwargs`` are passed to
        ``aiosqlite.connect``; ``table_name``, ``ttl``, ``timeout``,
        ``commit_interval``, and ``commit_max_pending`` configure the store and
        behave exactly as in :meth:`__init__`. :meth:`initialize` is called before
        the store is yielded. On exit the store is closed first (:meth:`aclose`,
        which flushes any write-behind buffer) and then the connection is closed —
        always, including when ``initialize`` or the body raises. Pass ``":memory:"``
        for an ephemeral database (note: it is gone once the context exits).

        Requires the ``sqlite`` extra (``pip install "mcp-persist[sqlite]"``); the
        import happens here, not at module import time, so the package loads
        without ``aiosqlite`` installed.
        """
        import aiosqlite

        conn = await aiosqlite.connect(path, **connect_kwargs)
        store = cls(
            conn,
            table_name=table_name,
            tenant_id=tenant_id,
            ttl=ttl,
            timeout=timeout,
            commit_interval=commit_interval,
            commit_max_pending=commit_max_pending,
        )
        try:
            await store.initialize()
            yield store
        finally:
            # Flush + stop the write-behind task before closing the connection;
            # committing on a closed connection would fail and lose buffered events.
            try:
                await store.aclose()
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
                        # A tenant-bound store needs the tenant_id column even on a
                        # table created by an older version; migrate it once here.
                        if self._tenant_id is not None:
                            await self._ensure_tenant_column()
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
                    "created_at REAL NOT NULL, "
                    "tenant_id TEXT)"
                )
                await self._conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {self._stream_index} ON {self._index_target} (stream_id, event_id)"
                )
                await self._conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {self._created_index} ON {self._index_target} (created_at)"
                )
                await self._conn.commit()
                # Bring a table created by an older version (no tenant_id column)
                # up to date. Idempotent and cheap: one pragma read at startup.
                await self._ensure_tenant_column()
            except sqlite3.OperationalError as exc:
                # IF NOT EXISTS handles the normal case; another connection winning a
                # concurrent create can still surface "already exists" or a locking error.
                # The object exists now, so treat it as done rather than crashing startup.
                exc_str = str(exc).lower()
                if "already exists" not in exc_str and "locked" not in exc_str:
                    raise
                logger.debug("Tolerating concurrent DDL race on %s: %s", self._table, exc)

            self._initialized = True

    def _tenant_filter_sql(self) -> tuple[str, tuple[Any, ...]]:
        """Return an ``AND tenant_id = ?`` clause (and its param) scoping a query.

        A store bound to a tenant scopes every read and write to its own rows; an
        unbound store (``tenant_id=None``) is unscoped and sees every tenant's
        events, so it returns an empty clause.
        """
        if self._tenant_id is None:
            return "", ()
        return " AND tenant_id = ?", (self._tenant_id,)

    async def _ensure_tenant_column(self) -> None:
        """Add the ``tenant_id`` column + index to a pre-1.9 table (idempotent).

        Fresh tables already declare ``tenant_id`` in ``CREATE TABLE``; this only
        does work the first time a tenant-bound store opens a table created by an
        older version. The result is cached so it runs at most once per store.
        """
        if self._tenant_column_ready:
            return
        table_parts = self._table.split(".")
        bare = table_parts[-1].strip('"')
        schema_prefix = f"{table_parts[0]}." if len(table_parts) == 2 else ""
        query = f"SELECT 1 FROM {schema_prefix}pragma_table_info('{bare}') WHERE name='tenant_id'"
        async with self._conn.execute(query) as cursor:
            row = await cursor.fetchone()
        if not row:
            await self._conn.execute(f"ALTER TABLE {self._table} ADD COLUMN tenant_id TEXT")
        await self._conn.execute(
            f'CREATE INDEX IF NOT EXISTS {schema_prefix}"{bare}_tenant_stream_idx" '
            f"ON {self._index_target} (tenant_id, stream_id, event_id)"
        )
        await self._conn.commit()
        self._tenant_column_ready = True

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
            payload = compress_payload(payload, codec=self._compression, min_bytes=self._compress_min_bytes)

        cols = "(stream_id, payload, created_at"
        vals = "(?, ?, ?"
        params: list[Any] = [stream_id, payload, time.time()]
        if self._tenant_id is not None:
            cols += ", tenant_id"
            vals += ", ?"
            params.append(self._tenant_id)
        cols += ")"
        vals += ")"

        if self._commit_interval is None and self._commit_max_pending is None:
            async with self._conn.execute(
                f"INSERT INTO {self._table} {cols} VALUES {vals}",
                params,
            ) as cursor:
                await self._conn.commit()
                return str(cursor.lastrowid)

        # Write-behind: leave the insert in SQLite's open transaction and let the
        # background flusher (or an inline max-pending commit) persist it later.
        # The commit lock guards the insert + counter so a concurrent flush can't
        # zero ``_pending`` while a row it didn't commit is still outstanding.
        self._ensure_flusher()
        async with self._commit_lock:
            async with self._conn.execute(
                f"INSERT INTO {self._table} {cols} VALUES {vals}",
                params,
            ) as cursor:
                event_id = str(cursor.lastrowid)
            self._pending += 1
            flush_now = self._commit_max_pending is not None and self._pending >= self._commit_max_pending
        if flush_now:
            await self._flush()
        return event_id

    # SQLite intentionally exposes no _allocate_event_ids / _store_event_with_id:
    # BatchingEventStore is for backends whose per-event round trip is the
    # bottleneck (Redis, Postgres). SQLite's own write-behind (commit_interval /
    # commit_max_pending) already batches the fsync that dominates its write cost,
    # so wrapping it in BatchingEventStore is rejected (see config.event_store_from_env
    # and BatchingEventStore.__init__).

    # Write-behind commits

    def _ensure_flusher(self) -> None:
        """Start the background commit loop on first write-behind write (idempotent).

        Lazy-started from the write path so it binds to the running event loop, and
        only when ``commit_interval`` is set. Restarts the task if it has somehow
        finished, so a one-off error can't permanently stop flushing.
        """
        if self._commit_interval is None:
            return
        if self._flush_task is not None and not self._flush_task.done():
            return
        self._flush_task = asyncio.create_task(self._commit_loop())

    async def _commit_loop(self) -> None:
        """Commit buffered write-behind inserts every ``commit_interval`` seconds."""
        assert self._commit_interval is not None
        while True:
            await asyncio.sleep(self._commit_interval)
            try:
                await self._flush()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep flushing across a transient commit error
                logger.exception("write-behind commit failed; will retry next interval")

    async def _flush(self) -> None:
        """Commit any buffered write-behind inserts; a no-op when none are pending.

        Holds ``_commit_lock`` so the commit and the ``_pending`` reset are atomic
        with respect to a concurrent ``store_event`` — at commit time every counted
        row has finished its insert and none is mid-flight, so resetting to ``0``
        can't drop an as-yet-uncommitted row.
        """
        async with self._commit_lock:
            if self._pending == 0:
                return
            await self._conn.commit()
            self._pending = 0

    async def aclose(self) -> None:
        """Stop the write-behind flusher and commit any still-buffered events.

        Idempotent, and safe to call when write-behind is off (it commits nothing).
        **Required** when ``commit_interval`` / ``commit_max_pending`` is set and you
        constructed the store directly: otherwise the last buffered events sit in an
        uncommitted transaction and are lost on shutdown. :meth:`create` and the
        ``async with`` form call this for you.
        """
        task = self._flush_task
        self._flush_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        try:
            await self._flush()
        except Exception:  # noqa: BLE001 - best-effort final flush; log rather than mask the close
            logger.exception(
                "final write-behind flush failed on close; up to one commit_interval of events may be lost"
            )

    async def __aenter__(self) -> SQLiteEventStore:
        await self.initialize()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

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

        tenant_sql, tenant_params = self._tenant_filter_sql()

        async with self._conn.execute(
            f"SELECT stream_id FROM {self._table} WHERE event_id = ?{tenant_sql}",
            (anchor_id, *tenant_params),
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            return None

        stream_id: StreamId = row[0]

        if self._ttl is not None:
            cutoff = time.time() - self._ttl
            # Detect an unrecoverable gap: the anchor still exists but one or more
            # client-visible events after it have expired and will be silently
            # skipped below, so the resuming client misses them with no other
            # signal. Priming events (empty payload) are never replayed, so an
            # expired one is not a gap and is excluded here.
            async with self._conn.execute(
                f"SELECT 1 FROM {self._table} "
                f"WHERE stream_id = ? AND event_id > ? AND created_at < ? AND payload <> ''{tenant_sql} LIMIT 1",
                (stream_id, anchor_id, cutoff, *tenant_params),
            ) as gap_cursor:
                if await gap_cursor.fetchone() is not None:
                    logger.warning(
                        "Replay gap on stream %s: one or more events after Last-Event-ID %s have expired "
                        "(ttl=%ss) and cannot be replayed; the resuming client will miss them.",
                        stream_id,
                        last_event_id,
                        self._ttl,
                    )
            query = (
                f"SELECT event_id, payload FROM {self._table} "
                f"WHERE stream_id = ? AND event_id > ? AND created_at >= ?{tenant_sql} "
                "ORDER BY event_id"
            )
            params: tuple[Any, ...] = (
                stream_id,
                anchor_id,
                cutoff,
                *tenant_params,
            )
        else:
            query = (
                f"SELECT event_id, payload FROM {self._table} "
                f"WHERE stream_id = ? AND event_id > ?{tenant_sql} ORDER BY event_id"
            )
            params = (stream_id, anchor_id, *tenant_params)

        # Stream rows one at a time rather than fetchall()-ing the whole backlog
        # into memory: a client resuming from a long-idle Last-Event-ID could
        # otherwise pull hundreds of thousands of rows into RAM at once.
        async with self._conn.execute(query, params) as cursor:
            async for event_id_int, payload in cursor:
                # Priming events (empty payload) are stored but never replayed.
                if not payload:
                    continue

                try:
                    message = jsonrpc_message_adapter.validate_json(decompress_payload(payload))
                except Exception as exc:  # noqa: BLE001 - corrupt payload (bad JSON or undecompressible); skip it, don't abort the stream
                    # A single corrupt/unparseable payload must not abort the whole
                    # replay: a reconnecting client would otherwise lose every event
                    # on the stream, not just the bad one. Skip it and keep going.
                    logger.warning(
                        "Skipping event %s on stream %s during replay: failed JSONRPC validation/decompression: %s",
                        event_id_int,
                        stream_id,
                        exc,
                    )
                    continue
                await send_callback(EventMessage(message=message, event_id=str(event_id_int)))

        return stream_id

    # Maintenance

    async def ping(self) -> bool:
        """Check the database connection is usable, for readiness/health probes.

        Runs a trivial ``SELECT 1``. Returns ``True`` on success and lets any
        driver error propagate (e.g. a closed connection), so a probe can treat a
        raised exception as "not ready".
        """
        async with self._conn.execute("SELECT 1"):
            return True

    async def select_expired(
        self,
        *,
        cutoff: float,
        batch_size: int,
    ) -> AsyncIterator[StoredEvent]:
        """Yield up to ``batch_size`` expired events without deleting them."""
        if batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size!r}")
        if not self._initialized:
            await self.initialize()

        tenant_sql, tenant_params = self._tenant_filter_sql()
        async with self._conn.execute(
            f"SELECT stream_id, event_id, payload, created_at FROM {self._table} "
            f"WHERE created_at < ?{tenant_sql} ORDER BY event_id LIMIT ?",
            (cutoff, *tenant_params, batch_size),
        ) as cursor:
            async for stream_id, event_id_int, payload, created_at in cursor:
                yield StoredEvent(
                    stream_id=stream_id,
                    event_id=str(event_id_int),
                    payload=payload,
                    created_at=created_at,
                )

    async def count_expired(self) -> int:
        """Return the number of events older than ``ttl`` without deleting them."""
        if self._ttl is None:
            return 0
        if not self._initialized:
            await self.initialize()
        cutoff = time.time() - self._ttl
        tenant_sql, tenant_params = self._tenant_filter_sql()
        async with self._conn.execute(
            f"SELECT COUNT(*) FROM {self._table} WHERE created_at < ?{tenant_sql}",
            (cutoff, *tenant_params),
        ) as cursor:
            row = await cursor.fetchone()
            return int(row[0])

    async def delete_events(self, events: Sequence[StoredEvent]) -> int:
        """Delete the given events by ``event_id`` and return the number removed."""
        if not events:
            return 0
        if not self._initialized:
            await self.initialize()
        ids = [int(event.event_id) for event in events]
        placeholders = ",".join("?" * len(ids))
        async with self._conn.execute(
            f"DELETE FROM {self._table} WHERE event_id IN ({placeholders})",
            ids,
        ) as cursor:
            await self._conn.commit()
            return cursor.rowcount

    async def _store_event_raw(
        self,
        stream_id: StreamId,
        event_id: EventId,
        payload: str,
        created_at: float,
    ) -> None:
        """Insert an event with an explicit ``event_id`` (upsert on conflict).

        Used by tiered archival to copy a hot event into a cold store while
        preserving its ID. A tenant-bound cold store tags the row with its tenant.
        """
        if not self._initialized:
            await self.initialize()
        await self._conn.execute(
            f"INSERT INTO {self._table} (event_id, stream_id, payload, created_at, tenant_id) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(event_id) DO UPDATE SET "
            "stream_id = excluded.stream_id, "
            "payload = excluded.payload, "
            "created_at = excluded.created_at, "
            "tenant_id = excluded.tenant_id",
            (int(event_id), stream_id, payload, created_at, self._tenant_id),
        )
        await self._conn.commit()

    async def _event_exists(self, event_id: EventId) -> bool:
        if not self._initialized:
            await self.initialize()
        try:
            anchor_id = int(event_id)
        except (TypeError, ValueError):
            return False
        tenant_sql, tenant_params = self._tenant_filter_sql()
        async with self._conn.execute(
            f"SELECT 1 FROM {self._table} WHERE event_id = ?{tenant_sql} LIMIT 1",
            (anchor_id, *tenant_params),
        ) as cursor:
            return await cursor.fetchone() is not None

    async def _stream_id_for_event(self, event_id: EventId) -> StreamId | None:
        if not self._initialized:
            await self.initialize()
        try:
            anchor_id = int(event_id)
        except (TypeError, ValueError):
            return None
        tenant_sql, tenant_params = self._tenant_filter_sql()
        async with self._conn.execute(
            f"SELECT stream_id FROM {self._table} WHERE event_id = ?{tenant_sql}",
            (anchor_id, *tenant_params),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row is not None else None

    async def purge_expired(self, *, batch_size: int | None = None) -> int:
        """Delete events older than ``ttl`` and return the number removed.

        No-op returning ``0`` when ``ttl`` is ``None``. SQLite has no automatic
        key expiry, so schedule this (e.g. from a periodic background task) to
        keep the database from growing without bound.

        Args:
            batch_size: When ``None`` (the default) every expired row is removed
                in a single ``DELETE``. When set to a positive integer, rows are
                deleted in chunks of that many, committing after each chunk, so a
                large purge does not hold one long write lock that contends with
                live inserts and replay scans. The expiry cutoff is captured once
                up front, so events that expire while the loop runs are left for
                the next call.
        """
        if self._ttl is None:
            return 0

        if batch_size is not None and batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer or None, got {batch_size!r}")

        if not self._initialized:
            await self.initialize()

        cutoff = time.time() - self._ttl
        tenant_sql, tenant_params = self._tenant_filter_sql()

        if batch_size is None:
            async with self._conn.execute(
                f"DELETE FROM {self._table} WHERE created_at < ?{tenant_sql}",
                (cutoff, *tenant_params),
            ) as cursor:
                await self._conn.commit()
                return cursor.rowcount

        total = 0
        while True:
            # SQLite only supports DELETE ... LIMIT when built with
            # SQLITE_ENABLE_UPDATE_DELETE_LIMIT (not the default), so bound the
            # batch via a subselect on the indexed event_id instead.
            async with self._conn.execute(
                f"DELETE FROM {self._table} WHERE event_id IN "
                f"(SELECT event_id FROM {self._table} WHERE created_at < ?{tenant_sql} ORDER BY event_id LIMIT ?)",
                (cutoff, *tenant_params, batch_size),
            ) as cursor:
                await self._conn.commit()
                removed = cursor.rowcount
            total += removed
            if removed < batch_size:
                break
        return total

    # Migration support

    async def list_streams(self) -> AsyncIterator[StreamId]:
        """Yield each distinct stream ID currently stored, in arbitrary order.

        Backs :func:`mcp_persist.migrate` for whole-database migrations.
        """
        if not self._initialized:
            await self.initialize()

        tenant_sql, tenant_params = self._tenant_filter_sql()
        # The tenant clause replaces the absent WHERE, so strip the leading " AND".
        where = f" WHERE {tenant_sql[5:]}" if tenant_sql else ""
        async with self._conn.execute(f"SELECT DISTINCT stream_id FROM {self._table}{where}", tenant_params) as cursor:
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

        tenant_sql, tenant_params = self._tenant_filter_sql()
        async with self._conn.execute(
            f"SELECT event_id, payload FROM {self._table} WHERE stream_id = ?{tenant_sql} ORDER BY event_id",
            (stream_id, *tenant_params),
        ) as cursor:
            async for event_id_int, payload in cursor:
                event_id = str(event_id_int)
                if not payload:
                    # Priming event: stored with an empty payload, copied as None.
                    yield event_id, None
                    continue

                try:
                    message = jsonrpc_message_adapter.validate_json(decompress_payload(payload))
                except Exception as exc:  # noqa: BLE001 - corrupt payload (bad JSON or undecompressible); skip it, don't abort the stream
                    logger.warning(
                        "Skipping event %s on stream %s during migration: failed JSONRPC validation/decompression: %s",
                        event_id,
                        stream_id,
                        exc,
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
                    message = jsonrpc_message_adapter.validate_json(decompress_payload(payload))
                except Exception as exc:  # noqa: BLE001 - corrupt payload (bad JSON or undecompressible); skip it, don't abort the stream
                    logger.warning(
                        "Skipping event %s on stream %s during subscribe: failed JSONRPC validation/decompression: %s",
                        event_id_int,
                        stream_id,
                        exc,
                    )
                    continue

                yield str(event_id_int), message

            await asyncio.sleep(poll_interval)
