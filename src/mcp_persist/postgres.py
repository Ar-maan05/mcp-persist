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
import hashlib
import logging
import re
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

from mcp_persist.compression import compress_payload, decompress_payload, validate_compression
from mcp_persist.metrics import NoOpMetricsCollector, safe_call

if TYPE_CHECKING:
    from mcp_persist.metrics import MetricsCollector

logger = logging.getLogger(__name__)

jsonrpc_message_adapter = TypeAdapter(JSONRPCMessage)

IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Default rows pulled per round-trip when replaying a backlog, so a client
# resuming from a very old Last-Event-ID can't materialize the whole stream in
# memory at once. Overridable per store via the ``replay_batch_size`` kwarg.
_DEFAULT_REPLAY_BATCH_SIZE = 500

# SQLSTATEs Postgres can raise when concurrent workers run the same
# ``CREATE ... IF NOT EXISTS`` at once: ``IF NOT EXISTS`` is not fully atomic
# against the system catalogs, so a racing creator can surface a duplicate or
# unique-violation error even though the object now exists. Treated as success.
_DUPLICATE_DDL_SQLSTATES = frozenset(
    {
        "42P07",  # duplicate_table
        "42P06",  # duplicate_schema
        "42710",  # duplicate_object (e.g. index)
        "23505",  # unique_violation (pg_class / pg_type catalog race)
    }
)


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
        table_name: Table name, optionally schema-qualified (``"schema.table"``).
                    Use different names when multiple MCP servers share one
                    database. Each dot-separated part must be a valid SQL
                    identifier. Default: ``"mcp_events"``.
        ttl:        Seconds after which events are considered expired and are
                    skipped on replay (and removed by :meth:`purge_expired`).
                    ``None`` means events never expire — discouraged in
                    production. PostgreSQL has no automatic row expiry, so call
                    :meth:`purge_expired` periodically (e.g. from a background
                    task or ``pg_cron``) to reclaim space.
        timeout:    Optional per-query timeout in seconds, passed through to
                    asyncpg. ``None`` (the default) waits indefinitely. Set it
                    so a query can't hang a request handler forever under lock
                    contention or database overload.
        replay_batch_size:
                    Rows fetched per round-trip when replaying a backlog
                    (default ``500``). Bounds replay memory so a client resuming
                    from a very old Last-Event-ID can't pull the whole stream
                    into memory at once. Lower it if your payloads are unusually
                    large; raise it to trade memory for fewer round-trips. Must
                    be a positive integer.
        metrics:    Optional :class:`~mcp_persist.metrics.MetricsCollector` for
                    timing/count hooks on ``store_event`` and
                    ``replay_events_after``. ``None`` (the default) installs a
                    no-op collector and the store takes a fast path with no
                    measurable overhead.
        enable_streaming:
                    When ``True``, ``store_event`` issues a ``pg_notify`` after
                    each non-priming write so :meth:`subscribe` can deliver
                    events in real time via ``LISTEN``/``NOTIFY``. ``False`` (the
                    default) means no extra statement per write and
                    :meth:`subscribe` raises if called. The notify is
                    best-effort: a failure is logged and never fails the write.
                    Note each active subscriber holds one pool connection for its
                    lifetime — size the pool accordingly (see
                    ``docs/production.md``).
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
    """

    def __init__(
        self,
        pool: Any,  # asyncpg.Pool at runtime
        *,
        table_name: str = "mcp_events",
        ttl: int | None = None,
        timeout: float | None = None,
        replay_batch_size: int = _DEFAULT_REPLAY_BATCH_SIZE,
        metrics: MetricsCollector | None = None,
        enable_streaming: bool = False,
        compression: str | None = None,
        compress_min_bytes: int = 1024,
    ) -> None:
        parts = table_name.split(".")
        if len(parts) > 2 or not all(part and IDENTIFIER_RE.match(part) for part in parts):
            raise ValueError(f"table_name must be a valid SQL identifier or 'schema.table', got {table_name!r}")
        if replay_batch_size < 1:
            raise ValueError(f"replay_batch_size must be a positive integer, got {replay_batch_size!r}")
        validate_compression(compression)
        if compress_min_bytes < 0:
            raise ValueError(f"compress_min_bytes must be a non-negative integer, got {compress_min_bytes!r}")

        self._pool = pool
        quoted_parts = [f'"{part}"' for part in parts]
        self._table = ".".join(quoted_parts)
        # Index names are created in the table's schema, so they are bare
        # identifiers derived from the unqualified table name.
        bare = parts[-1]
        self._stream_index = f'"{bare}_stream_idx"'
        self._created_index = f'"{bare}_created_idx"'
        self._ttl = ttl
        self._timeout = timeout
        self._replay_batch_size = replay_batch_size
        self._metrics: MetricsCollector = metrics if metrics is not None else NoOpMetricsCollector()
        self._enable_streaming = enable_streaming
        self._compression = compression
        self._compress_min_bytes = compress_min_bytes
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

    # Convenience constructor

    @classmethod
    @asynccontextmanager
    async def create(
        cls,
        dsn: str,
        *,
        table_name: str = "mcp_events",
        ttl: int | None = None,
        timeout: float | None = None,
        replay_batch_size: int = _DEFAULT_REPLAY_BATCH_SIZE,
        **pool_kwargs: Any,
    ) -> AsyncIterator[PostgresEventStore]:
        """Open an asyncpg pool, initialize, yield a store, and close it on exit.

        A convenience async context manager that owns the pool lifecycle so
        callers don't have to create, initialize, and close an ``asyncpg.Pool``
        themselves::

            async with PostgresEventStore.create("postgresql://localhost/mydb", ttl=3600) as store:
                await store.store_event(...)

        ``dsn`` and any extra ``pool_kwargs`` (e.g. ``min_size=``, ``max_size=``)
        are passed to ``asyncpg.create_pool``; ``table_name``, ``ttl``,
        ``timeout``, and ``replay_batch_size`` configure the store and behave
        exactly as in :meth:`__init__`. :meth:`initialize` is called before the
        store is yielded. The pool is always closed on exit, including when
        ``initialize`` or the body raises.

        Requires the ``postgres`` extra (``pip install "mcp-persist[postgres]"``);
        the import happens here, not at module import time, so the package loads
        without ``asyncpg`` installed.
        """
        import asyncpg

        pool = await asyncpg.create_pool(dsn, **pool_kwargs)
        store = cls(
            pool,
            table_name=table_name,
            ttl=ttl,
            timeout=timeout,
            replay_batch_size=replay_batch_size,
        )
        try:
            await store.initialize()
            yield store
        finally:
            await pool.close()

    # Schema

    async def _execute_ddl(self, statement: str) -> None:
        """Run a DDL statement, tolerating concurrent-creation races.

        The in-process ``_init_lock`` serializes DDL within one event loop, but
        multiple workers or pods can still run ``initialize()`` against the same
        database at the same instant. ``IF NOT EXISTS`` narrows but does not
        close the catalog race, so a duplicate/unique-violation SQLSTATE here
        means a peer won the race and the object now exists — treat it as done.
        """
        try:
            # Table/index creation can be slower than standard queries; use a generous 30s timeout
            # unless the user has configured an even larger custom timeout.
            timeout = max(30.0, self._timeout) if self._timeout is not None else 30.0
            await self._pool.execute(statement, timeout=timeout)
        except Exception as exc:  # noqa: BLE001 - re-raised unless it is a known DDL race
            if getattr(exc, "sqlstate", None) in _DUPLICATE_DDL_SQLSTATES:
                logger.debug("Tolerating concurrent DDL race on %s: %s", self._table, exc)
                return
            raise

    async def initialize(self) -> None:
        """Create the events table and indexes if they do not exist.

        Called automatically on first use; safe to call explicitly and
        repeatedly (e.g. at startup), and safe to run concurrently from multiple
        workers or pods — concurrent-creation races on the catalogs are
        tolerated. Creates a ``(stream_id, event_id)`` index for replay range
        scans and a ``created_at`` index so :meth:`purge_expired` can delete by
        age without a full table scan.
        """
        async with self._init_lock:
            if self._initialized:
                return
            await self._execute_ddl(
                f"CREATE TABLE IF NOT EXISTS {self._table} ("
                "event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                "stream_id TEXT NOT NULL, "
                "payload TEXT NOT NULL, "
                "created_at DOUBLE PRECISION NOT NULL)"
            )
            await self._execute_ddl(
                f"CREATE INDEX IF NOT EXISTS {self._stream_index} ON {self._table} (stream_id, event_id)"
            )
            await self._execute_ddl(f"CREATE INDEX IF NOT EXISTS {self._created_index} ON {self._table} (created_at)")
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
            payload = compress_payload(payload, codec=self._compression, min_bytes=self._compress_min_bytes)

        event_id = await self._pool.fetchval(
            f"INSERT INTO {self._table} (stream_id, payload, created_at) VALUES ($1, $2, $3) RETURNING event_id",
            stream_id,
            payload,
            time.time(),
            timeout=self._timeout,
        )

        event_id_str = str(event_id)

        # Notify real-time subscribers after the row is committed. Only for real
        # events (priming events carry no message and are not delivered by
        # subscribe). Best-effort: a notify failure must not fail the write.
        if self._enable_streaming and message is not None:
            await self._publish_notification(stream_id, event_id_str)

        return event_id_str

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

        row = await self._pool.fetchrow(
            f"SELECT stream_id FROM {self._table} WHERE event_id = $1",
            anchor_id,
            timeout=self._timeout,
        )

        if row is None:
            return None

        stream_id: StreamId = row["stream_id"]

        # Fetch and replay in bounded batches keyed on event_id rather than
        # pulling the whole backlog into memory at once. A client resuming from
        # a long-idle Last-Event-ID could otherwise materialize hundreds of
        # thousands of rows in one fetch() and OOM the worker.
        min_created = time.time() - self._ttl if self._ttl is not None else None
        cursor_id = anchor_id

        if min_created is not None:
            # Detect an unrecoverable gap: the anchor still exists but one or more
            # client-visible events after it have expired and will be silently
            # skipped below, so the resuming client misses them with no other
            # signal. Priming events (empty payload) are never replayed, so an
            # expired one is not a gap and is excluded here.
            gap = await self._pool.fetchval(
                f"SELECT 1 FROM {self._table} "
                "WHERE stream_id = $1 AND event_id > $2 AND created_at < $3 AND payload <> '' LIMIT 1",
                stream_id,
                anchor_id,
                min_created,
                timeout=self._timeout,
            )
            if gap is not None:
                logger.warning(
                    "Replay gap on stream %s: one or more events after Last-Event-ID %s have expired "
                    "(ttl=%ss) and cannot be replayed; the resuming client will miss them.",
                    stream_id,
                    last_event_id,
                    self._ttl,
                )

        while True:
            if min_created is not None:
                rows = await self._pool.fetch(
                    f"SELECT event_id, payload FROM {self._table} "
                    "WHERE stream_id = $1 AND event_id > $2 AND created_at >= $3 "
                    "ORDER BY event_id LIMIT $4",
                    stream_id,
                    cursor_id,
                    min_created,
                    self._replay_batch_size,
                    timeout=self._timeout,
                )
            else:
                rows = await self._pool.fetch(
                    f"SELECT event_id, payload FROM {self._table} "
                    "WHERE stream_id = $1 AND event_id > $2 ORDER BY event_id LIMIT $3",
                    stream_id,
                    cursor_id,
                    self._replay_batch_size,
                    timeout=self._timeout,
                )

            if not rows:
                break

            for record in rows:
                # Advance the cursor even past skipped priming events so the
                # next batch can't re-fetch them (no infinite loop).
                cursor_id = record["event_id"]
                payload = record["payload"]
                # Priming events (empty payload) are stored but never replayed.
                if not payload:
                    continue

                try:
                    message = jsonrpc_message_adapter.validate_json(decompress_payload(payload))
                except ValidationError:
                    # A single corrupt/unparseable payload must not abort the whole
                    # replay: a reconnecting client would otherwise lose every event
                    # on the stream, not just the bad one. Skip it and keep going.
                    logger.warning(
                        "Skipping event %s on stream %s during replay: payload failed JSONRPC validation",
                        record["event_id"],
                        stream_id,
                    )
                    continue
                await send_callback(EventMessage(message=message, event_id=str(record["event_id"])))

            if len(rows) < self._replay_batch_size:
                break

        return stream_id

    # Maintenance

    async def ping(self) -> bool:
        """Check the pool can reach the database, for readiness/health probes.

        Runs a trivial ``SELECT 1`` (honoring the store ``timeout``). Returns
        ``True`` on success and lets any driver error propagate, so a probe can
        treat a raised exception as "not ready".
        """
        await self._pool.fetchval("SELECT 1", timeout=self._timeout)
        return True

    async def purge_expired(self, *, batch_size: int | None = None) -> int:
        """Delete events older than ``ttl`` and return the number removed.

        No-op returning ``0`` when ``ttl`` is ``None``. PostgreSQL has no
        automatic row expiry, so schedule this (e.g. from a periodic background
        task or ``pg_cron``) to keep the table from growing without bound.
        (``pg_cron`` is a PostgreSQL extension that runs scheduled jobs inside
        the database itself, so cleanup can run without an external scheduler.)

        Args:
            batch_size: When ``None`` (the default) every expired row is removed
                in a single ``DELETE``. When set to a positive integer, rows are
                deleted in chunks of that many (via a ``ctid`` subselect) so a
                large purge does not hold one long lock that contends with live
                inserts and replay scans. The expiry cutoff is captured once up
                front, so events that expire while the loop runs are left for the
                next call.
        """
        if self._ttl is None:
            return 0

        if batch_size is not None and batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer or None, got {batch_size!r}")

        if not self._initialized:
            await self.initialize()

        cutoff = time.time() - self._ttl

        # asyncpg returns a command tag like "DELETE 5"; the count is the last token.
        # The created_at index added in initialize() keeps this an index scan
        # instead of a full table scan.
        if batch_size is None:
            result = await self._pool.execute(
                f"DELETE FROM {self._table} WHERE created_at < $1",
                cutoff,
                timeout=self._timeout,
            )
            return int(result.split()[-1])

        total = 0
        while True:
            result = await self._pool.execute(
                f"DELETE FROM {self._table} WHERE ctid IN "
                f"(SELECT ctid FROM {self._table} WHERE created_at < $1 ORDER BY created_at LIMIT $2)",
                cutoff,
                batch_size,
                timeout=self._timeout,
            )
            removed = int(result.split()[-1])
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

        rows = await self._pool.fetch(
            f"SELECT DISTINCT stream_id FROM {self._table}",
            timeout=self._timeout,
        )
        for record in rows:
            yield record["stream_id"]

    async def _iter_stream_events(self, stream_id: StreamId) -> AsyncIterator[tuple[EventId, JSONRPCMessage | None]]:
        """Yield ``(event_id, message)`` for every stored event on a stream, oldest first.

        Unlike :meth:`replay_events_after`, this enumerates the whole stream from
        the beginning (no anchor) and includes priming events (yielded as a
        ``None`` message), so :func:`mcp_persist.migrate` can copy a stream
        faithfully. Rows are fetched in ``replay_batch_size`` chunks so a large
        stream is not materialized at once, and are not filtered by ``ttl`` —
        run :meth:`purge_expired` first to drop stale events. A payload that
        fails JSONRPC validation is logged and skipped.
        """
        if not self._initialized:
            await self.initialize()

        # event_id is a positive IDENTITY column, so 0 is a safe lower bound that
        # includes the very first event (which replay_events_after cannot reach).
        cursor_id = 0

        while True:
            rows = await self._pool.fetch(
                f"SELECT event_id, payload FROM {self._table} "
                "WHERE stream_id = $1 AND event_id > $2 ORDER BY event_id LIMIT $3",
                stream_id,
                cursor_id,
                self._replay_batch_size,
                timeout=self._timeout,
            )

            if not rows:
                break

            for record in rows:
                cursor_id = record["event_id"]
                event_id = str(record["event_id"])
                payload = record["payload"]
                if not payload:
                    # Priming event: stored with an empty payload, copied as None.
                    yield event_id, None
                    continue

                try:
                    message = jsonrpc_message_adapter.validate_json(decompress_payload(payload))
                except ValidationError:
                    logger.warning(
                        "Skipping event %s on stream %s during migration: payload failed JSONRPC validation",
                        event_id,
                        stream_id,
                    )
                    continue

                yield event_id, message

            if len(rows) < self._replay_batch_size:
                break

    # Push-based streaming

    def _notify_channel(self, stream_id: StreamId) -> str:
        """Map a stream ID to a NOTIFY channel name within Postgres' 63-byte limit.

        Channel names are capped at 63 bytes, so a readable ``mcp_events_<id>``
        is used when it fits and a stable hash otherwise. The publisher and every
        subscriber compute this identically, so they always agree on the channel.
        """
        base = f"mcp_events_{stream_id}"
        if len(base.encode("utf-8")) <= 63:
            return base
        digest = hashlib.sha1(stream_id.encode("utf-8")).hexdigest()  # noqa: S324 - non-cryptographic channel name
        return f"mcp_events_{digest}"  # 11 + 40 = 51 bytes

    async def _publish_notification(self, stream_id: StreamId, event_id: EventId) -> None:
        """Send a ``pg_notify`` carrying the new event ID (best-effort)."""
        try:
            await self._pool.execute(
                "SELECT pg_notify($1, $2)",
                self._notify_channel(stream_id),
                event_id,
                timeout=self._timeout,
            )
        except Exception:  # noqa: BLE001 - notification is best-effort; never fail the write
            logger.warning(
                "Failed to send streaming notification for event %s on stream %s",
                event_id,
                stream_id,
                exc_info=True,
            )

    async def subscribe(self, stream_id: StreamId) -> AsyncIterator[tuple[EventId, JSONRPCMessage]]:
        """Yield ``(event_id, message)`` for events on a stream in real time.

        Requires ``enable_streaming=True``. Uses Postgres ``LISTEN``/``NOTIFY``::

            async for event_id, message in store.subscribe("stream-abc"):
                ...

        **Forward-only and best-effort (at-most-once).** Only events written
        *after* the subscription is established are delivered; use
        :meth:`replay_events_after` to catch up on history. ``NOTIFY`` is not
        buffered, so notifications emitted while no subscriber is listening (or
        during a reconnect) are missed — :meth:`replay_events_after` remains the
        durable path. Priming events and payloads that fail JSONRPC validation
        are skipped.

        **A dropped connection is not surfaced as an error.** The generator waits
        for the next ``NOTIFY``; if the underlying connection dies (e.g. the
        server restarts) no notification arrives and the ``async for`` goes quiet
        rather than raising. Don't treat silence as liveness — keep an
        application-level heartbeat on the session to detect a stalled
        subscription and reconnect, and use :meth:`replay_events_after` after any
        reconnect to close the gap.

        **Pool sizing.** Each active subscriber acquires one connection from the
        pool and holds it for the lifetime of the subscription (asyncpg requires
        a dedicated connection for ``LISTEN``). Size ``max_size`` for your peak
        number of concurrent subscribers *plus* normal store/replay traffic, or
        those operations can starve (see ``docs/production.md``). The generator is
        cancellable: breaking out of the ``async for`` (or cancelling the task)
        removes the listener and releases the connection.
        """
        if not self._enable_streaming:
            raise RuntimeError("subscribe() requires the store to be constructed with enable_streaming=True")

        if not self._initialized:
            await self.initialize()

        channel = self._notify_channel(stream_id)
        queue: asyncio.Queue[str] = asyncio.Queue()

        def listener(_connection: object, _pid: int, _channel: str, payload: str) -> None:
            queue.put_nowait(payload)

        conn = await self._pool.acquire()
        try:
            await conn.add_listener(channel, listener)
            while True:
                event_id = await queue.get()

                row = await conn.fetchrow(
                    f"SELECT payload FROM {self._table} WHERE event_id = $1",
                    int(event_id),
                    timeout=self._timeout,
                )
                if row is None:
                    continue

                payload = row["payload"]
                if not payload:
                    # Priming event; not delivered to subscribers.
                    continue

                try:
                    message = jsonrpc_message_adapter.validate_json(decompress_payload(payload))
                except ValidationError:
                    logger.warning(
                        "Skipping event %s on stream %s during subscribe: payload failed JSONRPC validation",
                        event_id,
                        stream_id,
                    )
                    continue

                yield event_id, message
        finally:
            # Release the connection unconditionally. remove_listener is wrapped
            # in its own try, and the release lives in a finally, so the pool
            # connection is returned even if remove_listener raises — including
            # CancelledError (a BaseException, not caught by `except Exception`),
            # which is exactly what arrives when an SSE client disconnects.
            try:
                await conn.remove_listener(channel, listener)
            except Exception:  # noqa: BLE001 - best-effort; the connection must still be released
                logger.debug("remove_listener failed during subscribe teardown", exc_info=True)
            finally:
                await self._pool.release(conn)
