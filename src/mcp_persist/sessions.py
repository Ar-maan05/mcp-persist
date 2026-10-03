"""Durable MCP session registry.

The SDK's :class:`~mcp.server.streamable_http_manager.StreamableHTTPSessionManager`
keeps its live sessions in ``self._server_instances``, a plain in-process dict.
A session id it has never seen gets a 404, which means a persistent event store
alone does not survive a restart: the events are still on disk, but the client
cannot get back to them because the session id is no longer recognized. The same
applies to a second worker that did not create the session.

A :class:`SessionRegistry` moves that registry into the same durable store the
events already live in, so any process can answer "is this session id real, and
whose is it?". :class:`~mcp_persist.session_manager.ResumableSessionManager` uses
it to adopt a known session instead of rejecting it.

A registry shares the connection of the event store it is built from, so it adds
no new connection, pool, or configuration: it inherits the store's tenant
binding, and for SQLite in particular it must share the connection rather than
open a second handle to the same file.

    from mcp_persist import session_registry_for

    registry = session_registry_for(store)
    await registry.initialize()

The record is deliberately small: a session id, when it was created and last
seen, whether it has been terminated, and the authorization context of the
principal that created it. Nothing about the conversation is stored here; the
events remain the event store's job.
"""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mcp.server.streamable_http import EventStore

logger = logging.getLogger(__name__)

DEFAULT_SESSION_TABLE = "mcp_sessions"


# Longest client label SessionRecord.client returns; a client chooses its own name.
_MAX_CLIENT_LABEL = 64


@dataclass(frozen=True)
class SessionRecord:
    """A session as the registry knows it.

    Attributes:
        session_id: The MCP session id (the ``Mcp-Session-Id`` header value).
        created_at: Unix timestamp of first registration.
        last_seen_at: Unix timestamp of the most recent request on the session.
        terminated: True once the session was explicitly ended (client DELETE,
            idle timeout, or an operator running ``mcp-persist sessions
            terminate``). A terminated session is never adopted again.
        owner: The ``AuthorizationContext`` of the principal that created the
            session, or None when the server runs unauthenticated. Adoption
            compares this against the requesting principal.
        handshake: The params of the client's ``initialize`` request, or None
            if they were not captured. A process that adopts the session
            restores the handshake from them, since the client will not send
            ``initialize`` again, and an uninitialized connection refuses every
            method but ``ping``.
    """

    session_id: str
    created_at: float
    last_seen_at: float
    terminated: bool = False
    owner: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    handshake: dict[str, Any] | None = None

    @property
    def client(self) -> str | None:
        """The client's ``name version`` from its recorded ``initialize``, or None.

        Both come from the client, so the text is made safe to print: control
        characters (a terminal escape sequence, say) are replaced and it is
        capped at :data:`_MAX_CLIENT_LABEL` characters.
        """
        info = (self.handshake or {}).get("clientInfo")
        if not isinstance(info, dict):
            return None
        parts = [str(info[k]) for k in ("name", "version") if isinstance(info.get(k), (str, int, float))]
        if not parts:
            return None
        label = "".join(c if c.isprintable() else "?" for c in " ".join(parts))
        return label if len(label) <= _MAX_CLIENT_LABEL else label[: _MAX_CLIENT_LABEL - 3] + "..."

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view, for the CLI and for logs."""
        return {
            "session_id": self.session_id,
            "created_at": self.created_at,
            "last_seen_at": self.last_seen_at,
            "terminated": self.terminated,
            "owner": self.owner,
            "metadata": self.metadata,
            "handshake": self.handshake,
            "client": self.client,
        }


class SessionRegistry(ABC):
    """Durable record of which session ids exist and who owns them."""

    @abstractmethod
    async def initialize(self) -> None:
        """Create whatever storage the registry needs. Idempotent."""

    @abstractmethod
    async def register(
        self, session_id: str, *, owner: dict[str, Any] | None = None, handshake: dict[str, Any] | None = None
    ) -> None:
        """Record a newly created session, with the client's ``initialize`` params when known."""

    @abstractmethod
    async def get(self, session_id: str) -> SessionRecord | None:
        """Return the record for ``session_id``, or None if unknown."""

    @abstractmethod
    async def touch(self, session_id: str) -> None:
        """Push ``last_seen_at`` forward. Silently ignores unknown ids."""

    @abstractmethod
    async def terminate(self, session_id: str) -> None:
        """Mark the session ended so it is never adopted again."""

    @abstractmethod
    async def list_sessions(self, *, include_terminated: bool = False, limit: int = 100) -> list[SessionRecord]:
        """Return sessions, most recently seen first."""

    @abstractmethod
    async def purge(self, *, older_than: float) -> int:
        """Delete sessions not seen for ``older_than`` seconds. Returns the count."""

    async def aclose(self) -> None:
        """Release registry-owned resources.

        The default is a no-op: a registry borrows the event store's connection
        and must not close it, since the store is still using it.
        """


def _owner_matches(stored: dict[str, Any] | None, requestor: dict[str, Any] | None) -> bool:
    """Whether a request's principal may use a session created by ``stored``.

    Both sides are ``AuthorizationContext`` dicts (``client_id``/``issuer``/
    ``subject``) or None for an unauthenticated server. The comparison is exact,
    matching what the SDK does in-process with ``!=`` on the context: an
    unauthenticated request may not pick up an authenticated session, and vice
    versa. Adopting a session across workers must not be a way around the
    credential binding.
    """
    if stored is None or requestor is None:
        return stored is None and requestor is None
    return all(stored.get(k) == requestor.get(k) for k in ("client_id", "issuer", "subject"))


class _SQLSessionRegistry(SessionRegistry):
    """Shared shape for the two SQL backends.

    Subclasses supply the parameter style and the execute/fetch primitives; the
    tenant column is carried so a multi-tenant deployment cannot see or adopt
    another tenant's sessions.
    """

    def __init__(self, *, tenant_id: str | None) -> None:
        self._tenant_id = tenant_id
        self._ready = False

    @staticmethod
    def _decode_json(raw: Any) -> dict[str, Any] | None:
        if raw is None:
            return None
        if isinstance(raw, dict):
            return raw
        try:
            decoded = json.loads(raw)
        except (TypeError, ValueError):
            logger.warning("Ignoring an unreadable JSON field on a session record")
            return None
        return decoded if isinstance(decoded, dict) else None

    @staticmethod
    def _encode_json(value: dict[str, Any] | None) -> str | None:
        return None if value is None else json.dumps(value, sort_keys=True)


class SQLiteSessionRegistry(_SQLSessionRegistry):
    """Session registry stored alongside the events in the same SQLite file."""

    def __init__(self, store: Any, *, table_name: str = DEFAULT_SESSION_TABLE) -> None:
        super().__init__(tenant_id=getattr(store, "_tenant_id", None))
        from mcp_persist.sqlite import IDENTIFIER_RE

        if not table_name or not IDENTIFIER_RE.match(table_name):
            raise ValueError(f"table_name must be a valid SQL identifier, got {table_name!r}")
        self._store = store
        self._conn = store._conn
        self._table = f'"{table_name}"'

    @property
    def _tenant_key(self) -> str:
        """ "" for "no tenant".

        SQLite treats NULLs as distinct in a PRIMARY KEY, so a NULL tenant made
        every ON CONFLICT miss: re-registering an id inserted a second row, and a
        terminated session could come back as a live one. Matches the Postgres
        registry and the record stores.
        """
        return self._tenant_id or ""

    async def initialize(self) -> None:
        if self._ready:
            return
        await self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "  session_id TEXT NOT NULL,"
            "  tenant_id TEXT NOT NULL DEFAULT '',"
            "  created_at REAL NOT NULL,"
            "  last_seen_at REAL NOT NULL,"
            "  terminated INTEGER NOT NULL DEFAULT 0,"
            "  owner TEXT,"
            "  handshake TEXT,"
            "  PRIMARY KEY (session_id, tenant_id)"
            ")"
        )
        await self._add_handshake_column()
        await self._migrate_null_tenants()
        await self._conn.commit()
        self._ready = True

    async def _add_handshake_column(self) -> None:
        """Add the ``handshake`` column to a table created before 2.2."""
        bare = self._table.strip('"')
        async with self._conn.execute(f"SELECT 1 FROM pragma_table_info('{bare}') WHERE name = 'handshake'") as cursor:
            if await cursor.fetchone() is None:
                await self._conn.execute(f"ALTER TABLE {self._table} ADD COLUMN handshake TEXT")

    async def _migrate_null_tenants(self) -> None:
        """Fold rows written with a NULL tenant (before 2.1.1) into the '' key.

        Those rows could hold several copies of one session id. They are merged
        so that a session ended in any copy stays ended: a merge must never be
        what revives a terminated session.
        """
        async with self._conn.execute(
            f"SELECT session_id, MIN(created_at), MAX(last_seen_at), MAX(terminated), MAX(owner) "
            f"FROM {self._table} WHERE tenant_id IS NULL GROUP BY session_id"
        ) as cursor:
            merged = await cursor.fetchall()
        if not merged:
            return
        await self._conn.execute(f"DELETE FROM {self._table} WHERE tenant_id IS NULL")
        await self._conn.executemany(
            f"INSERT INTO {self._table} (session_id, tenant_id, created_at, last_seen_at, terminated, owner) "
            "VALUES (?, '', ?, ?, ?, ?) "
            "ON CONFLICT(session_id, tenant_id) DO UPDATE SET "
            "created_at = MIN(created_at, excluded.created_at), "
            "last_seen_at = MAX(last_seen_at, excluded.last_seen_at), "
            "terminated = MAX(terminated, excluded.terminated)",
            merged,
        )

    async def register(
        self, session_id: str, *, owner: dict[str, Any] | None = None, handshake: dict[str, Any] | None = None
    ) -> None:
        await self.initialize()
        now = time.time()
        # A re-register of a live id refreshes it rather than resetting created_at,
        # and never silently revives a terminated session.
        await self._conn.execute(
            f"INSERT INTO {self._table} "
            "(session_id, tenant_id, created_at, last_seen_at, terminated, owner, handshake) "
            "VALUES (?, ?, ?, ?, 0, ?, ?) "
            "ON CONFLICT(session_id, tenant_id) DO UPDATE SET last_seen_at = excluded.last_seen_at, "
            "handshake = COALESCE(handshake, excluded.handshake)",
            (session_id, self._tenant_key, now, now, self._encode_json(owner), self._encode_json(handshake)),
        )
        await self._conn.commit()

    async def get(self, session_id: str) -> SessionRecord | None:
        await self.initialize()
        async with self._conn.execute(
            f"SELECT session_id, created_at, last_seen_at, terminated, owner, handshake FROM {self._table} "
            "WHERE session_id = ? AND tenant_id = ?",
            (session_id, self._tenant_key),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return SessionRecord(
            session_id=row[0],
            created_at=row[1],
            last_seen_at=row[2],
            terminated=bool(row[3]),
            owner=self._decode_json(row[4]),
            handshake=self._decode_json(row[5]),
        )

    async def touch(self, session_id: str) -> None:
        await self.initialize()
        await self._conn.execute(
            f"UPDATE {self._table} SET last_seen_at = ? WHERE session_id = ? AND tenant_id = ?",
            (time.time(), session_id, self._tenant_key),
        )
        await self._conn.commit()

    async def terminate(self, session_id: str) -> None:
        await self.initialize()
        await self._conn.execute(
            f"UPDATE {self._table} SET terminated = 1, last_seen_at = ? WHERE session_id = ? AND tenant_id = ?",
            (time.time(), session_id, self._tenant_key),
        )
        await self._conn.commit()

    async def list_sessions(self, *, include_terminated: bool = False, limit: int = 100) -> list[SessionRecord]:
        await self.initialize()
        query = (
            f"SELECT session_id, created_at, last_seen_at, terminated, owner, handshake FROM {self._table} "
            "WHERE tenant_id = ?"
        )
        params: list[Any] = [self._tenant_key]
        if not include_terminated:
            query += " AND terminated = 0"
        query += " ORDER BY last_seen_at DESC LIMIT ?"
        params.append(limit)
        async with self._conn.execute(query, tuple(params)) as cursor:
            rows = await cursor.fetchall()
        return [
            SessionRecord(
                session_id=r[0],
                created_at=r[1],
                last_seen_at=r[2],
                terminated=bool(r[3]),
                owner=self._decode_json(r[4]),
                handshake=self._decode_json(r[5]),
            )
            for r in rows
        ]

    async def purge(self, *, older_than: float) -> int:
        await self.initialize()
        cutoff = time.time() - older_than
        cursor = await self._conn.execute(
            f"DELETE FROM {self._table} WHERE last_seen_at < ? AND tenant_id = ?",
            (cutoff, self._tenant_key),
        )
        await self._conn.commit()
        return int(cursor.rowcount or 0)


class PostgresSessionRegistry(_SQLSessionRegistry):
    """Session registry stored alongside the events in the same database."""

    def __init__(self, store: Any, *, table_name: str = DEFAULT_SESSION_TABLE) -> None:
        super().__init__(tenant_id=getattr(store, "_tenant_id", None))
        from mcp_persist.postgres import IDENTIFIER_RE

        parts = table_name.split(".")
        if len(parts) > 2 or not all(p and IDENTIFIER_RE.match(p) for p in parts):
            raise ValueError(f"table_name must be a valid SQL identifier or 'schema.table', got {table_name!r}")
        self._store = store
        self._pool = store._pool
        self._table = ".".join(f'"{p}"' for p in parts)

    async def initialize(self) -> None:
        if self._ready:
            return
        await self._pool.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "  session_id TEXT NOT NULL,"
            "  tenant_id TEXT NOT NULL DEFAULT '',"
            "  created_at DOUBLE PRECISION NOT NULL,"
            "  last_seen_at DOUBLE PRECISION NOT NULL,"
            "  terminated BOOLEAN NOT NULL DEFAULT FALSE,"
            "  owner JSONB,"
            "  handshake JSONB,"
            "  PRIMARY KEY (session_id, tenant_id)"
            ")"
        )
        # Tables created before 2.2 have no handshake column.
        await self._pool.execute(f"ALTER TABLE {self._table} ADD COLUMN IF NOT EXISTS handshake JSONB")
        self._ready = True

    @property
    def _tenant_key(self) -> str:
        # Postgres treats NULL as distinct in a primary key, so the tenant
        # column uses '' for "no tenant" to keep the key usable.
        return self._tenant_id or ""

    async def register(
        self, session_id: str, *, owner: dict[str, Any] | None = None, handshake: dict[str, Any] | None = None
    ) -> None:
        await self.initialize()
        now = time.time()
        await self._pool.execute(
            f"INSERT INTO {self._table} "
            "(session_id, tenant_id, created_at, last_seen_at, terminated, owner, handshake) "
            "VALUES ($1, $2, $3, $4, FALSE, $5, $6) "
            "ON CONFLICT (session_id, tenant_id) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at, "
            f"handshake = COALESCE({self._table}.handshake, EXCLUDED.handshake)",
            session_id,
            self._tenant_key,
            now,
            now,
            self._encode_json(owner),
            self._encode_json(handshake),
        )

    async def get(self, session_id: str) -> SessionRecord | None:
        await self.initialize()
        row = await self._pool.fetchrow(
            f"SELECT session_id, created_at, last_seen_at, terminated, owner, handshake FROM {self._table} "
            "WHERE session_id = $1 AND tenant_id = $2",
            session_id,
            self._tenant_key,
        )
        if row is None:
            return None
        return SessionRecord(
            session_id=row["session_id"],
            created_at=row["created_at"],
            last_seen_at=row["last_seen_at"],
            terminated=bool(row["terminated"]),
            owner=self._decode_json(row["owner"]),
            handshake=self._decode_json(row["handshake"]),
        )

    async def touch(self, session_id: str) -> None:
        await self.initialize()
        await self._pool.execute(
            f"UPDATE {self._table} SET last_seen_at = $1 WHERE session_id = $2 AND tenant_id = $3",
            time.time(),
            session_id,
            self._tenant_key,
        )

    async def terminate(self, session_id: str) -> None:
        await self.initialize()
        await self._pool.execute(
            f"UPDATE {self._table} SET terminated = TRUE, last_seen_at = $1 WHERE session_id = $2 AND tenant_id = $3",
            time.time(),
            session_id,
            self._tenant_key,
        )

    async def list_sessions(self, *, include_terminated: bool = False, limit: int = 100) -> list[SessionRecord]:
        await self.initialize()
        query = (
            f"SELECT session_id, created_at, last_seen_at, terminated, owner, handshake FROM {self._table} "
            "WHERE tenant_id = $1"
        )
        if not include_terminated:
            query += " AND terminated = FALSE"
        query += " ORDER BY last_seen_at DESC LIMIT $2"
        rows = await self._pool.fetch(query, self._tenant_key, limit)
        return [
            SessionRecord(
                session_id=r["session_id"],
                created_at=r["created_at"],
                last_seen_at=r["last_seen_at"],
                terminated=bool(r["terminated"]),
                owner=self._decode_json(r["owner"]),
                handshake=self._decode_json(r["handshake"]),
            )
            for r in rows
        ]

    async def purge(self, *, older_than: float) -> int:
        await self.initialize()
        cutoff = time.time() - older_than
        result = await self._pool.execute(
            f"DELETE FROM {self._table} WHERE last_seen_at < $1 AND tenant_id = $2",
            cutoff,
            self._tenant_key,
        )
        # asyncpg returns the tag, e.g. "DELETE 3".
        try:
            return int(str(result).split()[-1])
        except (ValueError, IndexError):  # pragma: no cover - defensive
            return 0


class RedisSessionRegistry(SessionRegistry):
    """Session registry stored under the event store's key prefix.

    Each session is a HASH at ``{prefix}session:{id}`` and every session id is
    also a member of the ``{prefix}sessions`` ZSET scored by ``last_seen_at``,
    which is what makes "list by recency" and "purge by age" single commands.
    """

    def __init__(self, store: Any, *, ttl: int | None = None) -> None:
        self._store = store
        self._redis = store._redis
        self._prefix = store._prefix
        self._ttl = ttl

    def _key(self, session_id: str) -> str:
        return f"{self._prefix}session:{session_id}"

    @property
    def _index_key(self) -> str:
        return f"{self._prefix}sessions"

    async def initialize(self) -> None:
        """No schema to create; Redis keys are made on write."""

    async def register(
        self, session_id: str, *, owner: dict[str, Any] | None = None, handshake: dict[str, Any] | None = None
    ) -> None:
        now = time.time()
        key = self._key(session_id)
        # Every field that is fixed at creation is written with HSETNX inside one
        # MULTI/EXEC, so a re-register never reads the hash and writes it back.
        # A read-then-write let a `terminate` that landed in between be overwritten
        # with a stale terminated="0", reviving an ended session that could then
        # be adopted again. The SQL backends get the same from ON CONFLICT.
        # The owner is fixed at creation too: adoption compares the caller against
        # it, so a re-register must not be able to rebind the session.
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hsetnx(key, "session_id", session_id)
            pipe.hsetnx(key, "created_at", repr(now))
            pipe.hsetnx(key, "terminated", "0")
            pipe.hsetnx(key, "owner", json.dumps(owner, sort_keys=True) if owner is not None else "")
            if handshake is not None:
                # Like the SQL backends, a re-register keeps the handshake first recorded.
                pipe.hsetnx(key, "handshake", json.dumps(handshake, sort_keys=True))
            pipe.hset(key, "last_seen_at", repr(now))
            if self._ttl is not None:
                pipe.expire(key, self._ttl)
            pipe.zadd(self._index_key, {session_id: now})
            await pipe.execute()

    async def get(self, session_id: str) -> SessionRecord | None:
        raw = await self._redis.hgetall(self._key(session_id))
        if not raw:
            return None
        data = {_to_str(k): _to_str(v) for k, v in raw.items()}
        if not data.get("created_at"):
            # `register` always writes created_at. A hash without it is what a
            # touch or terminate leaves when the key expires between their
            # existence check and their write: not a session, and treating it as
            # one would bring an expired session back with no owner.
            return None
        owner_raw = data.get("owner") or ""
        owner: dict[str, Any] | None = None
        if owner_raw:
            try:
                decoded = json.loads(owner_raw)
                owner = decoded if isinstance(decoded, dict) else None
            except ValueError:  # pragma: no cover - defensive
                logger.warning("Ignoring unreadable owner payload on session %s", session_id[:64])
        return SessionRecord(
            session_id=data.get("session_id") or session_id,
            created_at=float(data.get("created_at") or 0.0),
            last_seen_at=float(data.get("last_seen_at") or 0.0),
            terminated=data.get("terminated") == "1",
            owner=owner,
            handshake=_json_object(data.get("handshake") or ""),
        )

    async def touch(self, session_id: str) -> None:
        key = self._key(session_id)
        if not await self._redis.exists(key):
            return
        now = time.time()
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.hset(key, "last_seen_at", repr(now))
            if self._ttl is not None:
                pipe.expire(key, self._ttl)
            pipe.zadd(self._index_key, {session_id: now})
            await pipe.execute()

    async def terminate(self, session_id: str) -> None:
        key = self._key(session_id)
        if not await self._redis.exists(key):
            return
        now = time.time()
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.hset(key, mapping={"terminated": "1", "last_seen_at": repr(now)})
            pipe.zadd(self._index_key, {session_id: now})
            await pipe.execute()

    async def list_sessions(self, *, include_terminated: bool = False, limit: int = 100) -> list[SessionRecord]:
        # Walk the index newest-first, skipping ids whose hash has expired out
        # from under it, and stop once `limit` live records have been collected.
        out: list[SessionRecord] = []
        stale: list[str] = []
        page = max(limit, 50)
        start = 0
        while len(out) < limit:
            ids = await self._redis.zrevrange(self._index_key, start, start + page - 1)
            if not ids:
                break
            start += page
            for raw_id in ids:
                session_id = _to_str(raw_id) or ""
                record = await self.get(session_id)
                if record is None:
                    stale.append(session_id)
                    continue
                if record.terminated and not include_terminated:
                    continue
                out.append(record)
                if len(out) >= limit:
                    break
        if stale:
            await self._redis.zrem(self._index_key, *stale)
        return out

    async def purge(self, *, older_than: float) -> int:
        cutoff = time.time() - older_than
        ids = await self._redis.zrangebyscore(self._index_key, "-inf", cutoff)
        if not ids:
            return 0
        names = [_to_str(i) or "" for i in ids]
        async with self._redis.pipeline(transaction=False) as pipe:
            for name in names:
                pipe.delete(self._key(name))
            pipe.zrem(self._index_key, *names)
            await pipe.execute()
        return len(names)


def _json_object(raw: str) -> dict[str, Any] | None:
    """Decode a JSON object stored as text, or None if absent or unreadable."""
    if not raw:
        return None
    try:
        decoded = json.loads(raw)
    except ValueError:
        logger.warning("Ignoring an unreadable JSON field on a session record")
        return None
    return decoded if isinstance(decoded, dict) else None


def _to_str(value: Any) -> str | None:
    """Normalize a Redis reply to str, for clients with or without decode_responses."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value if value is None or isinstance(value, str) else str(value)


def _unwrap(store: Any) -> Any:
    """Follow wrapper stores down to the backend that owns a connection.

    ``BatchingEventStore`` and ``ChainedEventStore`` delegate storage; the
    registry belongs on whatever backend is underneath.
    """
    seen: set[int] = set()
    current = store
    while id(current) not in seen:
        seen.add(id(current))
        inner = getattr(current, "_inner", None) or getattr(current, "_hot", None)
        if inner is None:
            break
        current = inner
    return current


def session_registry_for(
    store: EventStore,
    *,
    table_name: str = DEFAULT_SESSION_TABLE,
    ttl: int | None = None,
) -> SessionRegistry:
    """Build the registry matching ``store``'s backend, sharing its connection.

    Args:
        store: An open SQLite, Redis or Postgres event store. Wrapper stores
            (batching, tiered) are unwrapped to the backend underneath.
        table_name: Table for the SQL backends. Ignored by Redis, which keys off
            the store's own prefix.
        ttl: Redis-only expiry for a session key, in seconds.

    Raises:
        TypeError: If the backend has no registry implementation.
    """
    from mcp_persist.postgres import PostgresEventStore
    from mcp_persist.redis import RedisEventStore
    from mcp_persist.sqlite import SQLiteEventStore

    target = _unwrap(store)
    if isinstance(target, SQLiteEventStore):
        return SQLiteSessionRegistry(target, table_name=table_name)
    if isinstance(target, PostgresEventStore):
        return PostgresSessionRegistry(target, table_name=table_name)
    if isinstance(target, RedisEventStore):
        return RedisSessionRegistry(target, ttl=ttl)
    raise TypeError(
        f"no session registry for {type(target).__name__}; durable sessions need a sqlite, redis or postgres store"
    )
