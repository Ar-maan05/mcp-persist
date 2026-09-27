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

For MCPServer servers, ``with_persistence`` wires a store into a runnable ASGI app
in one call (see :mod:`mcp_persist.fastmcp`):

    from mcp_persist import with_persistence

    app = with_persistence(mcp, backend="sqlite", url="events.db", ttl=3600)

To add resumability without modifying the server, ``PersistenceProxy`` (and the
``mcp-persist-proxy`` CLI) fronts any upstream MCP endpoint and stores its SSE
events (see :mod:`mcp_persist.proxy`).
"""

from importlib.metadata import PackageNotFoundError, version

from mcp_persist._debug import configure_debug_logging
from mcp_persist.batching import BatchingEventStore
from mcp_persist.config import event_store_from_env, retention_policy_from_env
from mcp_persist.encryption import KeyRing, generate_key, keyring_from_env
from mcp_persist.fastmcp import with_persistence
from mcp_persist.health import HealthReport
from mcp_persist.metrics import (
    LoggingMetricsCollector,
    MetricsCollector,
    NoOpMetricsCollector,
)
from mcp_persist.migration import MigrationResult, migrate
from mcp_persist.portability import export_stream, import_stream
from mcp_persist.postgres import PostgresEventStore
from mcp_persist.proxy import PersistenceProxy
from mcp_persist.recorder import RecordFlusher
from mcp_persist.records import (
    Carrier,
    Outcome,
    PayloadPolicy,
    PostgresRecordStore,
    Record,
    RecordStore,
    RedisRecordStore,
    SQLiteRecordStore,
    record_store_for,
    require_payload_encryption,
)
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
from mcp_persist.session_manager import ResumableSessionManager
from mcp_persist.sessions import (
    PostgresSessionRegistry,
    RedisSessionRegistry,
    SessionRecord,
    SessionRegistry,
    SQLiteSessionRegistry,
    session_registry_for,
)
from mcp_persist.sqlite import SQLiteEventStore
from mcp_persist.stored import StoredEvent, archive_expired_batch, count_expired
from mcp_persist.tiered import ChainedEventStore

try:
    __version__ = version("mcp-persist")
except PackageNotFoundError:  # pragma: no cover - running from a source tree without install
    __version__ = "0.0.0+unknown"

# Honour DEBUG_PERSIST as early as import so the FLUSH/PURGE lines from stores
# built with a custom metrics collector still reach stderr. No-op unless the
# flag is set (see mcp_persist._debug).
configure_debug_logging()

__all__ = [
    "ArchiveScheduler",
    "AuditSink",
    "BatchingEventStore",
    "Carrier",
    "ChainedEventStore",
    "DatabaseAuditSink",
    "DeletionAuditEntry",
    "HealthReport",
    "KeyRing",
    "LoggingAuditSink",
    "LoggingMetricsCollector",
    "MetricsCollector",
    "MigrationResult",
    "NoOpAuditSink",
    "NoOpMetricsCollector",
    "Outcome",
    "PayloadPolicy",
    "PersistenceProxy",
    "PostgresEventStore",
    "PostgresRecordStore",
    "PostgresSessionRegistry",
    "PurgeScheduler",
    "Record",
    "RecordFlusher",
    "RecordStore",
    "RedisEventStore",
    "RedisRecordStore",
    "RedisSessionRegistry",
    "ResumableSessionManager",
    "RetentionPolicy",
    "RetentionScheduler",
    "SQLiteEventStore",
    "SQLiteRecordStore",
    "SQLiteSessionRegistry",
    "SessionRecord",
    "SessionRegistry",
    "StoredEvent",
    "__version__",
    "archive_expired_batch",
    "count_expired",
    "event_store_from_env",
    "export_stream",
    "generate_key",
    "import_stream",
    "keyring_from_env",
    "migrate",
    "record_store_for",
    "require_payload_encryption",
    "retention_policy_from_env",
    "session_registry_for",
    "with_persistence",
]
