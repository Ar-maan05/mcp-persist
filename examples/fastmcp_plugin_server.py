"""
examples/fastmcp_plugin_server.py
=================================
A FastMCP server made resumable in one line with ``with_persistence``.

This is the same kind of server as ``sqlite_server.py`` / ``redis_server.py``,
but instead of hand-wiring an ``aiosqlite`` connection, a ``SQLiteEventStore``,
a ``StreamableHTTPSessionManager`` and a Starlette lifespan, the whole thing
collapses to a single ``with_persistence(...)`` call that returns a ready ASGI
app and owns the store lifecycle for you.

Install and run (uvicorn ships with mcp, so one install is enough):
    pip install "mcp-persist[sqlite]"
    python examples/fastmcp_plugin_server.py

The server exposes an MCP endpoint at http://localhost:8000/mcp
"""

from __future__ import annotations

import asyncio

import uvicorn
from mcp.server.fastmcp import FastMCP

from mcp_persist import with_persistence

# ---------------------------------------------------------------------------
# MCP server — define tools exactly as you normally would with FastMCP
# ---------------------------------------------------------------------------

mcp = FastMCP(name="EchoServer")


@mcp.tool()
def shout(message: str) -> dict[str, str]:
    """Uppercase a message."""
    return {"shout": message.upper()}


@mcp.tool()
async def slow_echo(message: str, delay: float = 1.0) -> dict[str, object]:
    """Echo a message after a delay — useful for observing SSE keepalives."""
    await asyncio.sleep(delay)
    return {"echo": message, "delay": delay}


# ---------------------------------------------------------------------------
# Persistence in one line.
# ---------------------------------------------------------------------------
#
# Pattern A — config kwargs (used below): the store is built and its lifecycle
# is owned by the returned app.
app = with_persistence(mcp, backend="sqlite", url="echo_events.db", ttl=3600)

# Other backends are a one-word change:
#   app = with_persistence(mcp, backend="redis", url="redis://localhost:6379", ttl=3600)
#   app = with_persistence(mcp, backend="postgres", url="postgresql://localhost/mcp", ttl=3600)
#
# Pattern B — bring your own store (you own its lifecycle):
#   async with SQLiteEventStore.create("echo_events.db", ttl=3600) as store:
#       app = with_persistence(mcp, store=store)
#       await uvicorn.Server(uvicorn.Config(app, port=8000)).serve()
#
# Pattern C — configure from the environment (MCP_PERSIST_BACKEND / _URL / _TTL):
#   app = with_persistence(mcp)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
