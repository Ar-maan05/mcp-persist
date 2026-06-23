"""mcp-persist: Production-grade persistence backends for the MCP Python SDK.

Currently ships:
    RedisEventStore    — Redis-backed EventStore for SSE stream resumability
                         across multi-process / multi-worker deployments.
    SQLiteEventStore   — SQLite-backed EventStore for single-node durability
                         across process restarts, with no external service.
    PostgresEventStore — PostgreSQL-backed EventStore for durable resumability
                         on deployments already running Postgres, including
                         multi-node / team setups.

Usage:
    pip install "mcp-persist[redis]"     # or [sqlite] / [postgres]

    from mcp_persist import RedisEventStore, SQLiteEventStore, PostgresEventStore

For FastMCP servers, ``with_persistence`` wires a store into a runnable ASGI app
in one call (see :mod:`mcp_persist.fastmcp`):

    from mcp_persist import with_persistence

    app = with_persistence(mcp, backend="sqlite", url="events.db", ttl=3600)

To add resumability without modifying the server, ``PersistenceProxy`` (and the
``mcp-persist-proxy`` CLI) fronts any upstream MCP endpoint and stores its SSE
events (see :mod:`mcp_persist.proxy`).
"""

from importlib.metadata import PackageNotFoundError, version

from mcp_persist.batching import BatchingEventStore
from mcp_persist.config import event_store_from_env, retention_policy_from_env
from mcp_persist.fastmcp import with_persistence
from mcp_persist.metrics import (
    LoggingMetricsCollector,
    MetricsCollector,
    NoOpMetricsCollector,
)
from mcp_persist.migration import MigrationResult, migrate
from mcp_persist.postgres import PostgresEventStore
from mcp_persist.proxy import PersistenceProxy
from mcp_persist.redis import RedisEventStore
from mcp_persist.retention import (
    AuditSink,
    DatabaseAuditSink,
    DeletionAuditEntry,
    LoggingAuditSink,
    NoOpAuditSink,
    RetentionPolicy,
)
from mcp_persist.scheduler import ArchiveScheduler, PurgeScheduler, RetentionScheduler
from mcp_persist.sqlite import SQLiteEventStore
from mcp_persist.stored import StoredEvent, archive_expired_batch, count_expired
from mcp_persist.tiered import ChainedEventStore

try:
    __version__ = version("mcp-persist")
except PackageNotFoundError:  # pragma: no cover - running from a source tree without install
    __version__ = "0.0.0+unknown"

__all__ = [
    "ArchiveScheduler",
    "AuditSink",
    "BatchingEventStore",
    "ChainedEventStore",
    "DatabaseAuditSink",
    "DeletionAuditEntry",
    "LoggingAuditSink",
    "LoggingMetricsCollector",
    "MetricsCollector",
    "MigrationResult",
    "NoOpAuditSink",
    "NoOpMetricsCollector",
    "PersistenceProxy",
    "PostgresEventStore",
    "PurgeScheduler",
    "RedisEventStore",
    "RetentionPolicy",
    "RetentionScheduler",
    "SQLiteEventStore",
    "StoredEvent",
    "__version__",
    "archive_expired_batch",
    "count_expired",
    "event_store_from_env",
    "migrate",
    "retention_policy_from_env",
    "with_persistence",
]
