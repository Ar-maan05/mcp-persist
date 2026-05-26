"""mcp-persist: Production-grade persistence backends for the MCP Python SDK.

Currently ships:
    RedisEventStore — Redis-backed EventStore for SSE stream resumability
                      across multi-process/multi-worker deployments.

Usage:
    pip install "mcp-persist[redis]"

    from mcp_persist import RedisEventStore
"""

from mcp_persist.redis import RedisEventStore

__all__ = ["RedisEventStore"]
