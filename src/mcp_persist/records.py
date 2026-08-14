"""Durable records of what a server handled, on every protocol era.

The event store persists the SSE frames a client may need replayed, which the
SDK only consults on the handshake-era transport: a ``2026-07-28`` request is a
self-contained POST that never reaches the session path at all. A *record* is
the other half. It is a small, durable, semantic note of one handled message -
which method, which era, how it ended, how long it took - written on every
protocol era, so a store keeps telling you what happened as a deployment
migrates across revisions.

Records are deliberately a separate surface from events. They share the
backend, the tenant binding, the compression and encryption codecs, and the
retention machinery, but they live in their own table (or key prefix) with
their own ttl. Conflating the two would break both the replay contract and the
``dump``/``load`` format.

    from mcp_persist import record_store_for

    records = record_store_for(store)
    await records.initialize()
    await records.store_record(record)

A record is never a replay token and can never be used to resume a stream.

Two safety properties are structural here rather than left to callers:

* There is no free-text error field. A failure is a fixed :data:`Outcome` plus
  an optional numeric ``error_code``. Validation messages and exception strings
  echo user input, so the type system simply gives them nowhere to go.
* Payload capture is an allowlist and defaults to off. See :class:`PayloadPolicy`.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from mcp.server.streamable_http import EventStore

logger = logging.getLogger(__name__)

DEFAULT_RECORD_TABLE = "mcp_records"

Outcome = Literal[
    "ok",
    "tool_error",
    "mcp_error",
    "validation_error",
    "exception",
    "cancelled",
]
"""How a handled message ended.

``ok`` is a successful handler return. ``tool_error`` is a ``tools/call`` that
returned ``isError`` (a tool failing is not a transport failure, and the two
must stay distinguishable). ``mcp_error`` and ``validation_error`` are the two
raised shapes the SDK's own middleware separates, and ``exception`` is anything
else. ``cancelled`` is a client disconnect: the modern transport cancels the
handler's task group, so the work did not finish.
"""

OUTCOMES: frozenset[str] = frozenset(("ok", "tool_error", "mcp_error", "validation_error", "exception", "cancelled"))

Carrier = Literal["middleware", "proxy"]
"""Which carrier observed the message: in-process middleware, or the proxy."""

Kind = Literal["request", "notification"]


@dataclass(frozen=True)
class Record:
    """One handled message, as the record store knows it.

    Attributes:
        record_id: Unique id for this record. Generated when omitted.
        recorded_at: Unix timestamp the record was created.
        protocol_version: The negotiated protocol revision, straight from
            ``ServerRequestContext.protocol_version``. This is what lets one
            store span a migration and still be readable per era.
        method: The JSON-RPC method, e.g. ``tools/call``.
        kind: ``request`` or ``notification``.
        outcome: See :data:`Outcome`.
        carrier: See :data:`Carrier`.
        duration_ms: Wall time spent handling, when known.
        request_id: The JSON-RPC request id as a string, or None for a
            notification.
        tool_name: For ``tools/call``, the tool that was invoked.
        error_code: A numeric JSON-RPC error code for the failing outcomes.
            There is deliberately no accompanying message field.
        payload: Policy-selected fragments only, never raw params. None unless
            a :class:`PayloadPolicy` explicitly allowed something.
        payload_truncated: True when the policy shortened or dropped a value to
            stay inside its size caps, so a reader can tell a clipped value from
            a genuinely short one.
    """

    protocol_version: str
    method: str
    kind: Kind = "request"
    outcome: Outcome = "ok"
    carrier: Carrier = "middleware"
    record_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    recorded_at: float = field(default_factory=time.time)
    duration_ms: float | None = None
    request_id: str | None = None
    tool_name: str | None = None
    error_code: int | None = None
    payload: dict[str, Any] | None = None
    payload_truncated: bool = False

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view, for the CLI, dashboard and logs."""
        return {
            "record_id": self.record_id,
            "recorded_at": self.recorded_at,
            "protocol_version": self.protocol_version,
            "method": self.method,
            "kind": self.kind,
            "outcome": self.outcome,
            "carrier": self.carrier,
            "duration_ms": self.duration_ms,
            "request_id": self.request_id,
            "tool_name": self.tool_name,
            "error_code": self.error_code,
            "payload": self.payload,
            "payload_truncated": self.payload_truncated,
        }


class PayloadPolicy:
    """An allowlist deciding which params, if any, are ever written down.

    ``tools/call`` arguments are arbitrary third-party input: credentials, API
    keys, personal data, whole documents. So capture is an allowlist and the
    default is to capture nothing. A denylist of sensitive-looking key names
    cannot work here, because the field names belong to tool authors we have
    never met.

    Four rules make the allowlist safe to hand to an operator:

    * **Top-level scalars only.** Allowing ``config`` when its value is a nested
      object would quietly capture the entire subtree, including whatever a tool
      author nested inside it. A bare name therefore matches only scalar values.
      To reach into an object, name the exact path: ``config.region``.
    * **Omission, not masking.** A field that is not allowed is absent, rather
      than present with a placeholder value. The key name alone can reveal
      sensitive semantics (``patient_ssn`` tells you plenty even as ``"***"``).
    * **Truncation before storage**, so a capped value is what reaches the
      compression and encryption codecs, not merely what is displayed.
    * **A whole-record cap**, applied after per-value caps, so a wide object
      cannot add up to an unbounded row.

    Even used correctly this cannot promise that an allowed field is free of
    secrets. It narrows what is captured to what an operator explicitly named;
    the judgement about those fields stays with them.

    Args:
        tool_arguments: Allowed argument names per tool name, for ``tools/call``.
        method_params: Allowed param names per method, for everything else.
        max_value_bytes: Per-value cap, in UTF-8 bytes.
        max_record_bytes: Cap on the whole selected payload, in UTF-8 bytes.
    """

    __slots__ = ("_tool_arguments", "_method_params", "_max_value_bytes", "_max_record_bytes")

    def __init__(
        self,
        *,
        tool_arguments: Mapping[str, Sequence[str]] | None = None,
        method_params: Mapping[str, Sequence[str]] | None = None,
        max_value_bytes: int = 4096,
        max_record_bytes: int = 16384,
    ) -> None:
        if max_value_bytes <= 0 or max_record_bytes <= 0:
            raise ValueError("max_value_bytes and max_record_bytes must be positive")
        self._tool_arguments = {k: tuple(v) for k, v in (tool_arguments or {}).items()}
        self._method_params = {k: tuple(v) for k, v in (method_params or {}).items()}
        self._max_value_bytes = max_value_bytes
        self._max_record_bytes = max_record_bytes

    @classmethod
    def off(cls) -> PayloadPolicy:
        """The default: capture nothing at all."""
        return cls()

    @property
    def is_off(self) -> bool:
        """True when no field anywhere is allowed, so nothing can be captured."""
        return not self._tool_arguments and not self._method_params

    def select(
        self, method: str, params: Mapping[str, Any] | None, *, tool_name: str | None = None
    ) -> tuple[dict[str, Any] | None, bool]:
        """Return the allowed fragments of ``params``, and whether anything was clipped.

        Args:
            method: The JSON-RPC method being handled.
            params: The raw, pre-validation params. Never stored as given.
            tool_name: The tool being invoked, for ``tools/call``.

        Returns:
            ``(payload, truncated)``. ``payload`` is None when nothing is
            allowed, which is the default for every method.
        """
        if self.is_off or not params:
            return None, False

        if method == "tools/call":
            allowed = self._tool_arguments.get(tool_name or "", ())
            source = params.get("arguments")
        else:
            allowed = self._method_params.get(method, ())
            source = params
        if not allowed or not isinstance(source, Mapping):
            return None, False

        selected: dict[str, Any] = {}
        truncated = False
        for name in allowed:
            found, present = _resolve_path(source, name)
            if not present:
                continue
            value, clipped = self._cap(found)
            if value is _DROP:
                truncated = True
                continue
            selected[name] = value
            truncated = truncated or clipped

        if not selected:
            return None, truncated

        selected, dropped = self._cap_record(selected)
        return (selected or None), (truncated or dropped)

    def _cap(self, value: Any) -> tuple[Any, bool]:
        """Apply the per-value cap, dropping anything that is not a scalar."""
        if value is None or isinstance(value, (bool, int, float)):
            return value, False
        if not isinstance(value, str):
            # A dot-path may still land on an object or list. Capturing it would
            # mean capturing fields nobody named, so it is dropped, not encoded.
            return _DROP, True
        raw = value.encode("utf-8")
        if len(raw) <= self._max_value_bytes:
            return value, False
        return raw[: self._max_value_bytes].decode("utf-8", "ignore"), True

    def _cap_record(self, selected: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """Drop fields, in a stable order, until the whole payload fits."""
        if len(json.dumps(selected, sort_keys=True).encode("utf-8")) <= self._max_record_bytes:
            return selected, False
        kept = dict(sorted(selected.items()))
        while kept and len(json.dumps(kept, sort_keys=True).encode("utf-8")) > self._max_record_bytes:
            kept.popitem()
        return kept, True


class _Drop:
    """Sentinel for a value the policy refuses to capture."""

    __slots__ = ()


_DROP = _Drop()


def _resolve_path(source: Mapping[str, Any], name: str) -> tuple[Any, bool]:
    """Resolve a bare name or a dotted path against ``source``.

    A bare name matches only a top-level key. A dotted path walks nested
    mappings, so reaching inside an object always requires naming it explicitly.
    Returns ``(value, present)`` so a stored ``None`` is distinguishable from a
    missing key.
    """
    if "." not in name:
        return (source.get(name), name in source)
    current: Any = source
    for part in name.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None, False
        current = current[part]
    return current, True


def require_payload_encryption(store: Any, policy: PayloadPolicy, *, allow_plaintext: bool = False) -> None:
    """Refuse to capture payloads into a store that cannot encrypt them.

    Selected params are the most sensitive thing this library ever writes, so
    turning capture on without a keyring is treated as a configuration error
    rather than a silent downgrade. An operator who genuinely wants plaintext
    records passes ``allow_plaintext=True`` and owns that choice.

    Raises:
        ValueError: If ``policy`` captures anything, the store has no keyring,
            and plaintext was not explicitly accepted.
    """
    if policy.is_off or allow_plaintext:
        return
    if getattr(_unwrap(store), "_keyring", None) is None:
        raise ValueError(
            "payload capture is configured but the store has no keyring, so params would be "
            "written in plaintext; pass a keyring= to the store (see docs/encryption.md), "
            "turn capture off, or pass allow_plaintext=True to accept plaintext records"
        )


class RecordStore(ABC):
    """Durable store of :class:`Record` values, alongside the events."""

    @abstractmethod
    async def initialize(self) -> None:
        """Create whatever storage the records need. Idempotent."""

    @abstractmethod
    async def store_record(self, record: Record) -> None:
        """Write one record."""

    @abstractmethod
    async def store_records(self, records: Sequence[Record]) -> int:
        """Write many records, returning how many were written."""

    @abstractmethod
    async def list_records(
        self,
        *,
        limit: int = 100,
        method: str | None = None,
        outcome: str | None = None,
        since: float | None = None,
    ) -> list[Record]:
        """Return records, newest first."""

    @abstractmethod
    async def count(self) -> int:
        """Return how many records are stored for this tenant."""

    @abstractmethod
    async def purge(self, *, older_than: float) -> int:
        """Delete records older than ``older_than`` seconds. Returns the count."""

    async def aclose(self) -> None:
        """Release store-owned resources.

        A no-op by default: a record store borrows the event store's connection
        and must never close it, since the event store is still using it.
        """


class _SQLRecordStore(RecordStore):
    """Shared shape for the two SQL backends.

    Subclasses supply the parameter style and the execute/fetch primitives. The
    tenant column is carried so one tenant can never read another's records.
    """

    def __init__(self, store: Any, *, tenant_id: str | None) -> None:
        self._store = store
        self._tenant_id = tenant_id
        self._ready = False

    def _encode_payload(self, payload: dict[str, Any] | None) -> str | None:
        """Serialize then hand to the store's own codec (compress, then encrypt)."""
        if payload is None:
            return None
        return self._store._encode_payload(json.dumps(payload, sort_keys=True))

    def _decode_payload(self, stored: Any) -> dict[str, Any] | None:
        if stored is None:
            return None
        try:
            decoded = json.loads(self._store._decode_payload(stored))
        except Exception:
            # An unreadable payload (wrong key after a rotation, corruption)
            # must not lose the rest of the record, which is the part that says
            # what happened.
            logger.warning("Ignoring unreadable record payload")
            return None
        return decoded if isinstance(decoded, dict) else None


class SQLiteRecordStore(_SQLRecordStore):
    """Records stored alongside the events in the same SQLite file."""

    def __init__(self, store: Any, *, table_name: str = DEFAULT_RECORD_TABLE) -> None:
        super().__init__(store, tenant_id=getattr(store, "_tenant_id", None))
        from mcp_persist.sqlite import IDENTIFIER_RE

        if not table_name or not IDENTIFIER_RE.match(table_name):
            raise ValueError(f"table_name must be a valid SQL identifier, got {table_name!r}")
        self._conn = store._conn
        self._table = f'"{table_name}"'
        self._index = f'"{table_name}_recorded_at_idx"'

    async def initialize(self) -> None:
        if self._ready:
            return
        await self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "  record_id TEXT NOT NULL,"
            "  tenant_id TEXT,"
            "  recorded_at REAL NOT NULL,"
            "  protocol_version TEXT NOT NULL,"
            "  method TEXT NOT NULL,"
            "  kind TEXT NOT NULL,"
            "  outcome TEXT NOT NULL,"
            "  carrier TEXT NOT NULL,"
            "  duration_ms REAL,"
            "  request_id TEXT,"
            "  tool_name TEXT,"
            "  error_code INTEGER,"
            "  payload TEXT,"
            "  payload_truncated INTEGER NOT NULL DEFAULT 0,"
            "  PRIMARY KEY (record_id, tenant_id)"
            ")"
        )
        await self._conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self._index} ON {self._table} (tenant_id, recorded_at DESC)"
        )
        await self._conn.commit()
        self._ready = True

    def _row(self, record: Record) -> tuple[Any, ...]:
        return (
            record.record_id,
            self._tenant_id,
            record.recorded_at,
            record.protocol_version,
            record.method,
            record.kind,
            record.outcome,
            record.carrier,
            record.duration_ms,
            record.request_id,
            record.tool_name,
            record.error_code,
            self._encode_payload(record.payload),
            1 if record.payload_truncated else 0,
        )

    @property
    def _insert_sql(self) -> str:
        return (
            f"INSERT OR REPLACE INTO {self._table} ("
            "  record_id, tenant_id, recorded_at, protocol_version, method, kind, outcome,"
            "  carrier, duration_ms, request_id, tool_name, error_code, payload, payload_truncated"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )

    async def store_record(self, record: Record) -> None:
        await self.initialize()
        await self._conn.execute(self._insert_sql, self._row(record))
        await self._conn.commit()

    async def store_records(self, records: Sequence[Record]) -> int:
        if not records:
            return 0
        await self.initialize()
        await self._conn.executemany(self._insert_sql, [self._row(r) for r in records])
        await self._conn.commit()
        return len(records)

    async def list_records(
        self,
        *,
        limit: int = 100,
        method: str | None = None,
        outcome: str | None = None,
        since: float | None = None,
    ) -> list[Record]:
        await self.initialize()
        query = f"SELECT {_COLUMNS} FROM {self._table} WHERE tenant_id IS ?"
        params: list[Any] = [self._tenant_id]
        if method is not None:
            query += " AND method = ?"
            params.append(method)
        if outcome is not None:
            query += " AND outcome = ?"
            params.append(outcome)
        if since is not None:
            query += " AND recorded_at >= ?"
            params.append(since)
        query += " ORDER BY recorded_at DESC LIMIT ?"
        params.append(limit)
        async with self._conn.execute(query, tuple(params)) as cursor:
            rows = await cursor.fetchall()
        return [self._to_record(r) for r in rows]

    async def count(self) -> int:
        await self.initialize()
        async with self._conn.execute(
            f"SELECT COUNT(*) FROM {self._table} WHERE tenant_id IS ?", (self._tenant_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def purge(self, *, older_than: float) -> int:
        await self.initialize()
        cutoff = time.time() - older_than
        cursor = await self._conn.execute(
            f"DELETE FROM {self._table} WHERE recorded_at < ? AND tenant_id IS ?",
            (cutoff, self._tenant_id),
        )
        await self._conn.commit()
        return int(cursor.rowcount or 0)

    def _to_record(self, row: Sequence[Any]) -> Record:
        return Record(
            record_id=row[0],
            recorded_at=row[1],
            protocol_version=row[2],
            method=row[3],
            kind=row[4],
            outcome=row[5],
            carrier=row[6],
            duration_ms=row[7],
            request_id=row[8],
            tool_name=row[9],
            error_code=row[10],
            payload=self._decode_payload(row[11]),
            payload_truncated=bool(row[12]),
        )


_COLUMNS = (
    "record_id, recorded_at, protocol_version, method, kind, outcome, carrier, "
    "duration_ms, request_id, tool_name, error_code, payload, payload_truncated"
)


class PostgresRecordStore(_SQLRecordStore):
    """Records stored alongside the events in the same database."""

    def __init__(self, store: Any, *, table_name: str = DEFAULT_RECORD_TABLE) -> None:
        super().__init__(store, tenant_id=getattr(store, "_tenant_id", None))
        from mcp_persist.postgres import IDENTIFIER_RE

        parts = table_name.split(".")
        if len(parts) > 2 or not all(p and IDENTIFIER_RE.match(p) for p in parts):
            raise ValueError(f"table_name must be a valid SQL identifier or 'schema.table', got {table_name!r}")
        self._pool = store._pool
        self._table = ".".join(f'"{p}"' for p in parts)
        self._index = f'"{parts[-1]}_recorded_at_idx"'

    @property
    def _tenant_key(self) -> str:
        # Postgres treats NULL as distinct in a primary key, so "no tenant" is
        # stored as '' to keep the key usable. Matches the session registry.
        return self._tenant_id or ""

    async def initialize(self) -> None:
        if self._ready:
            return
        await self._pool.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "  record_id TEXT NOT NULL,"
            "  tenant_id TEXT NOT NULL DEFAULT '',"
            "  recorded_at DOUBLE PRECISION NOT NULL,"
            "  protocol_version TEXT NOT NULL,"
            "  method TEXT NOT NULL,"
            "  kind TEXT NOT NULL,"
            "  outcome TEXT NOT NULL,"
            "  carrier TEXT NOT NULL,"
            "  duration_ms DOUBLE PRECISION,"
            "  request_id TEXT,"
            "  tool_name TEXT,"
            "  error_code INTEGER,"
            "  payload TEXT,"
            "  payload_truncated BOOLEAN NOT NULL DEFAULT FALSE,"
            "  PRIMARY KEY (record_id, tenant_id)"
            ")"
        )
        await self._pool.execute(
            f"CREATE INDEX IF NOT EXISTS {self._index} ON {self._table} (tenant_id, recorded_at DESC)"
        )
        self._ready = True

    def _row(self, record: Record) -> tuple[Any, ...]:
        return (
            record.record_id,
            self._tenant_key,
            record.recorded_at,
            record.protocol_version,
            record.method,
            record.kind,
            record.outcome,
            record.carrier,
            record.duration_ms,
            record.request_id,
            record.tool_name,
            record.error_code,
            self._encode_payload(record.payload),
            record.payload_truncated,
        )

    @property
    def _insert_sql(self) -> str:
        return (
            f"INSERT INTO {self._table} ("
            "  record_id, tenant_id, recorded_at, protocol_version, method, kind, outcome,"
            "  carrier, duration_ms, request_id, tool_name, error_code, payload, payload_truncated"
            ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) "
            "ON CONFLICT (record_id, tenant_id) DO NOTHING"
        )

    async def store_record(self, record: Record) -> None:
        await self.initialize()
        await self._pool.execute(self._insert_sql, *self._row(record))

    async def store_records(self, records: Sequence[Record]) -> int:
        if not records:
            return 0
        await self.initialize()
        await self._pool.executemany(self._insert_sql, [self._row(r) for r in records])
        return len(records)

    async def list_records(
        self,
        *,
        limit: int = 100,
        method: str | None = None,
        outcome: str | None = None,
        since: float | None = None,
    ) -> list[Record]:
        await self.initialize()
        query = f"SELECT {_COLUMNS} FROM {self._table} WHERE tenant_id = $1"
        params: list[Any] = [self._tenant_key]
        if method is not None:
            params.append(method)
            query += f" AND method = ${len(params)}"
        if outcome is not None:
            params.append(outcome)
            query += f" AND outcome = ${len(params)}"
        if since is not None:
            params.append(since)
            query += f" AND recorded_at >= ${len(params)}"
        params.append(limit)
        query += f" ORDER BY recorded_at DESC LIMIT ${len(params)}"
        rows = await self._pool.fetch(query, *params)
        return [
            Record(
                record_id=r["record_id"],
                recorded_at=r["recorded_at"],
                protocol_version=r["protocol_version"],
                method=r["method"],
                kind=r["kind"],
                outcome=r["outcome"],
                carrier=r["carrier"],
                duration_ms=r["duration_ms"],
                request_id=r["request_id"],
                tool_name=r["tool_name"],
                error_code=r["error_code"],
                payload=self._decode_payload(r["payload"]),
                payload_truncated=bool(r["payload_truncated"]),
            )
            for r in rows
        ]

    async def count(self) -> int:
        await self.initialize()
        row = await self._pool.fetchrow(
            f"SELECT COUNT(*) AS n FROM {self._table} WHERE tenant_id = $1", self._tenant_key
        )
        return int(row["n"]) if row else 0

    async def purge(self, *, older_than: float) -> int:
        await self.initialize()
        cutoff = time.time() - older_than
        result = await self._pool.execute(
            f"DELETE FROM {self._table} WHERE recorded_at < $1 AND tenant_id = $2",
            cutoff,
            self._tenant_key,
        )
        # asyncpg returns the command tag, e.g. "DELETE 3".
        try:
            return int(str(result).split()[-1])
        except (ValueError, IndexError):  # pragma: no cover - defensive
            return 0


class RedisRecordStore(RecordStore):
    """Records stored under the event store's key prefix.

    Each record is a HASH at ``{prefix}record:{id}``, and every id is a member
    of the ``{prefix}records`` ZSET scored by ``recorded_at``. The index is what
    makes "newest first" and "purge by age" single commands, exactly as the
    session registry does it.
    """

    def __init__(self, store: Any, *, ttl: int | None = None) -> None:
        self._store = store
        self._redis = store._redis
        self._prefix = store._prefix
        self._ttl = ttl

    def _key(self, record_id: str) -> str:
        return f"{self._prefix}record:{record_id}"

    @property
    def _index_key(self) -> str:
        return f"{self._prefix}records"

    async def initialize(self) -> None:
        """No schema to create; Redis keys are made on write."""

    def _mapping(self, record: Record) -> dict[str, str]:
        payload = record.payload
        encoded = "" if payload is None else self._store._encode_payload(json.dumps(payload, sort_keys=True))
        return {
            "record_id": record.record_id,
            "recorded_at": repr(record.recorded_at),
            "protocol_version": record.protocol_version,
            "method": record.method,
            "kind": record.kind,
            "outcome": record.outcome,
            "carrier": record.carrier,
            "duration_ms": "" if record.duration_ms is None else repr(record.duration_ms),
            "request_id": record.request_id or "",
            "tool_name": record.tool_name or "",
            "error_code": "" if record.error_code is None else str(record.error_code),
            "payload": encoded,
            "payload_truncated": "1" if record.payload_truncated else "0",
        }

    async def store_record(self, record: Record) -> None:
        await self.store_records([record])

    async def store_records(self, records: Sequence[Record]) -> int:
        if not records:
            return 0
        async with self._redis.pipeline(transaction=False) as pipe:
            for record in records:
                key = self._key(record.record_id)
                pipe.hset(key, mapping=self._mapping(record))
                if self._ttl is not None:
                    pipe.expire(key, self._ttl)
                pipe.zadd(self._index_key, {record.record_id: record.recorded_at})
            await pipe.execute()
        return len(records)

    async def _get(self, record_id: str) -> Record | None:
        raw = await self._redis.hgetall(self._key(record_id))
        if not raw:
            return None
        data = {_to_str(k): _to_str(v) or "" for k, v in raw.items()}
        payload: dict[str, Any] | None = None
        if data.get("payload"):
            try:
                decoded = json.loads(self._store._decode_payload(data["payload"]))
                payload = decoded if isinstance(decoded, dict) else None
            except Exception:
                logger.warning("Ignoring unreadable payload on record %s", record_id[:64])
        return Record(
            record_id=data.get("record_id") or record_id,
            recorded_at=float(data.get("recorded_at") or 0.0),
            protocol_version=data.get("protocol_version") or "",
            method=data.get("method") or "",
            kind=data.get("kind") or "request",  # type: ignore[arg-type]
            outcome=data.get("outcome") or "ok",  # type: ignore[arg-type]
            carrier=data.get("carrier") or "middleware",  # type: ignore[arg-type]
            duration_ms=float(data["duration_ms"]) if data.get("duration_ms") else None,
            request_id=data.get("request_id") or None,
            tool_name=data.get("tool_name") or None,
            error_code=int(data["error_code"]) if data.get("error_code") else None,
            payload=payload,
            payload_truncated=data.get("payload_truncated") == "1",
        )

    async def list_records(
        self,
        *,
        limit: int = 100,
        method: str | None = None,
        outcome: str | None = None,
        since: float | None = None,
    ) -> list[Record]:
        # Walk the index newest-first, dropping ids whose hash has expired out
        # from under it, and stop once `limit` matching records are collected.
        out: list[Record] = []
        stale: list[str] = []
        page = max(limit, 50)
        start = 0
        min_score = "-inf" if since is None else since
        while len(out) < limit:
            ids = await self._redis.zrevrangebyscore(self._index_key, "+inf", min_score, start=start, num=page)
            if not ids:
                break
            start += page
            for raw_id in ids:
                record_id = _to_str(raw_id) or ""
                record = await self._get(record_id)
                if record is None:
                    stale.append(record_id)
                    continue
                if method is not None and record.method != method:
                    continue
                if outcome is not None and record.outcome != outcome:
                    continue
                out.append(record)
                if len(out) >= limit:
                    break
        if stale:
            await self._redis.zrem(self._index_key, *stale)
        return out

    async def count(self) -> int:
        return int(await self._redis.zcard(self._index_key))

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


def _to_str(value: Any) -> str | None:
    """Normalize a Redis reply to str, for clients with or without decode_responses."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value if value is None or isinstance(value, str) else str(value)


def _unwrap(store: Any) -> Any:
    """Follow wrapper stores down to the backend that owns a connection.

    ``BatchingEventStore`` and ``ChainedEventStore`` delegate storage; the
    record store belongs on whatever backend is underneath.
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


def record_store_for(
    store: EventStore,
    *,
    table_name: str = DEFAULT_RECORD_TABLE,
    ttl: int | None = None,
) -> RecordStore:
    """Build the record store matching ``store``'s backend, sharing its connection.

    Args:
        store: An open SQLite, Redis or Postgres event store. Wrapper stores
            (batching, tiered) are unwrapped to the backend underneath.
        table_name: Table for the SQL backends. Ignored by Redis, which keys off
            the store's own prefix.
        ttl: Record retention for Redis, in seconds. This is the record ttl and
            is deliberately separate from the event ttl: records usually outlive
            the events they describe.

    Raises:
        TypeError: If the backend has no record store implementation.
    """
    from mcp_persist.postgres import PostgresEventStore
    from mcp_persist.redis import RedisEventStore
    from mcp_persist.sqlite import SQLiteEventStore

    target = _unwrap(store)
    if isinstance(target, SQLiteEventStore):
        return SQLiteRecordStore(target, table_name=table_name)
    if isinstance(target, PostgresEventStore):
        return PostgresRecordStore(target, table_name=table_name)
    if isinstance(target, RedisEventStore):
        return RedisRecordStore(target, ttl=ttl)
    raise TypeError(f"no record store for {type(target).__name__}; records need a sqlite, redis or postgres store")
