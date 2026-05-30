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
"""

from importlib.metadata import PackageNotFoundError, version

from mcp_persist.postgres import PostgresEventStore
from mcp_persist.redis import RedisEventStore
from mcp_persist.sqlite import SQLiteEventStore

try:
    __version__ = version("mcp-persist")
except PackageNotFoundError:  # pragma: no cover - running from a source tree without install
    __version__ = "0.0.0+unknown"

__all__ = ["PostgresEventStore", "RedisEventStore", "SQLiteEventStore", "__version__"]
