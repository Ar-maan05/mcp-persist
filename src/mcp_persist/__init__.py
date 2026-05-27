"""mcp-persist: Production-grade persistence backends for the MCP Python SDK.

Currently ships:
    RedisEventStore  — Redis-backed EventStore for SSE stream resumability
                       across multi-process / multi-worker deployments.
    SQLiteEventStore — SQLite-backed EventStore for single-node durability
                       across process restarts, with no external service.

Usage:
    pip install "mcp-persist[redis]"     # or [sqlite]

    from mcp_persist import RedisEventStore, SQLiteEventStore
"""

from mcp_persist.redis import RedisEventStore
from mcp_persist.sqlite import SQLiteEventStore

__all__ = ["RedisEventStore", "SQLiteEventStore"]
