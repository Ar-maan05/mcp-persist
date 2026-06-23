"""Per-team retention policies with audit logging.

This module provides the classes and protocols needed to configure per-tenant
retention windows and record audit logs of deletions.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class RetentionPolicy:
    """Policy defining retention windows for different tenants.

    Windows map tenant_id to retention window in seconds. The default window
    applies to any tenant not explicitly present in windows.
    """

    windows: Mapping[str | None, int]
    default: int | None = None

    def __post_init__(self) -> None:
        cloned_windows = dict(self.windows)
        for k, v in cloned_windows.items():
            if not isinstance(v, int) or isinstance(v, bool) or v <= 0:
                raise ValueError("All window values must be integers greater than 0")
        if self.default is not None:
            if not isinstance(self.default, int) or isinstance(self.default, bool) or self.default <= 0:
                raise ValueError("default window must be an integer greater than 0")
        object.__setattr__(self, "windows", cloned_windows)

    def window_for(self, tenant_id: str | None) -> int | None:
        """Return the retention window in seconds for a tenant, or None to skip it."""
        if tenant_id in self.windows:
            return self.windows[tenant_id]
        return self.default


@dataclass(frozen=True)
class DeletionAuditEntry:
    """Audit log entry capturing details of a tenant purge cycle."""

    timestamp: float
    tenant_id: str | None
    window_seconds: int
    cutoff: float
    deleted_count: int
    backend: str
    source_table: str
    default_applied: bool

    def to_dict(self) -> dict[str, Any]:
        """Return a plain JSON-serializable dictionary matching the fields."""
        return {
            "timestamp": self.timestamp,
            "tenant_id": self.tenant_id,
            "window_seconds": self.window_seconds,
            "cutoff": self.cutoff,
            "deleted_count": self.deleted_count,
            "backend": self.backend,
            "source_table": self.source_table,
            "default_applied": self.default_applied,
        }


class AuditSink(Protocol):
    """Protocol for pluggable audit record destinations."""

    async def record(self, entry: DeletionAuditEntry) -> None:
        """Record an audit entry."""
        ...


class NoOpAuditSink:
    """Audit sink that discards all entries."""

    async def record(self, entry: DeletionAuditEntry) -> None:
        """Discard the entry."""
        pass


class LoggingAuditSink:
    """Audit sink that writes entries as JSON lines to a python logger."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("mcp_persist.audit")

    async def record(self, entry: DeletionAuditEntry) -> None:
        """Log the audit entry."""
        self._logger.info(json.dumps(entry.to_dict()))


class DatabaseAuditSink:
    """Audit sink that writes entries to an append-only database table."""

    def __init__(self, store: Any, *, audit_table: str | None = None) -> None:
        self._store = store
        self._backend = getattr(store, "backend_name", None)
        if self._backend not in ("sqlite", "postgres"):
            raise TypeError("DatabaseAuditSink requires a SQLiteEventStore or PostgresEventStore")

        if audit_table is None:
            # Derive from events table name
            source_table = store.table_name
            parts = source_table.split(".")
            table_part = parts[-1]
            is_quoted = table_part.startswith('"') and table_part.endswith('"')
            if is_quoted:
                bare = table_part[1:-1]
                new_table_part = f'"{bare}_retention_audit"'
            else:
                new_table_part = f"{table_part}_retention_audit"
            parts[-1] = new_table_part
            self._audit_table = ".".join(parts)
        else:
            self._audit_table = audit_table

        self._initialized = False

    async def _initialize_table(self) -> None:
        if self._initialized:
            return

        if self._backend == "sqlite":
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {self._audit_table} (\n"
                "    id             INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                "    ts             REAL    NOT NULL,\n"
                "    tenant_id      TEXT,\n"
                "    window_seconds INTEGER NOT NULL,\n"
                "    cutoff         REAL    NOT NULL,\n"
                "    deleted_count  INTEGER NOT NULL,\n"
                "    backend        TEXT    NOT NULL,\n"
                "    source_table   TEXT    NOT NULL,\n"
                "    default_applied INTEGER NOT NULL\n"
                ")"
            )
            await self._store._conn.execute(ddl)
            await self._store._conn.commit()
        elif self._backend == "postgres":
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {self._audit_table} (\n"
                "    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n"
                "    ts             DOUBLE PRECISION NOT NULL,\n"
                "    tenant_id      TEXT,\n"
                "    window_seconds INTEGER NOT NULL,\n"
                "    cutoff         DOUBLE PRECISION NOT NULL,\n"
                "    deleted_count  INTEGER NOT NULL,\n"
                "    backend        TEXT NOT NULL,\n"
                "    source_table   TEXT NOT NULL,\n"
                "    default_applied BOOLEAN NOT NULL\n"
                ")"
            )
            # Use PostgresEventStore's internal execution tool
            await self._store._execute_ddl(ddl)

        self._initialized = True

    async def record(self, entry: DeletionAuditEntry) -> None:
        """Write the audit entry into the database table."""
        if not self._initialized:
            await self._initialize_table()

        if self._backend == "sqlite":
            query = (
                f"INSERT INTO {self._audit_table} "
                "(ts, tenant_id, window_seconds, cutoff, deleted_count, backend, source_table, default_applied) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )
            params = (
                entry.timestamp,
                entry.tenant_id,
                entry.window_seconds,
                entry.cutoff,
                entry.deleted_count,
                entry.backend,
                entry.source_table,
                1 if entry.default_applied else 0,
            )
            await self._store._conn.execute(query, params)
            await self._store._conn.commit()
        elif self._backend == "postgres":
            query = (
                f"INSERT INTO {self._audit_table} "
                "(ts, tenant_id, window_seconds, cutoff, deleted_count, backend, source_table, default_applied) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
            )
            params = [
                entry.timestamp,
                entry.tenant_id,
                entry.window_seconds,
                entry.cutoff,
                entry.deleted_count,
                entry.backend,
                entry.source_table,
                entry.default_applied,
            ]
            # Use PostgreSQL query executor
            timeout = self._store._timeout
            await self._store._pool.execute(query, *params, timeout=timeout)
