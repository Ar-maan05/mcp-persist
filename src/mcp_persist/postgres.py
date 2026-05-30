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
import re
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
from pydantic import TypeAdapter, ValidationError

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
    """

    def __init__(
        self,
        pool: Any,  # asyncpg.Pool at runtime
        *,
        table_name: str = "mcp_events",
        ttl: int | None = None,
        timeout: float | None = None,
        replay_batch_size: int = _DEFAULT_REPLAY_BATCH_SIZE,
    ) -> None:
        parts = table_name.split(".")
        if len(parts) > 2 or not all(part and IDENTIFIER_RE.match(part) for part in parts):
            raise ValueError(f"table_name must be a valid SQL identifier or 'schema.table', got {table_name!r}")
        if replay_batch_size < 1:
            raise ValueError(f"replay_batch_size must be a positive integer, got {replay_batch_size!r}")

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
            timeout=self._timeout,
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
                    message = jsonrpc_message_adapter.validate_json(payload)
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
        # The created_at index added in initialize() keeps this an index scan
        # instead of a full table scan.
        result = await self._pool.execute(
            f"DELETE FROM {self._table} WHERE created_at < $1",
            time.time() - self._ttl,
            timeout=self._timeout,
        )
        return int(result.split()[-1])
