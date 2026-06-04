# Examples

Minimal, runnable MCP servers showing how to add `mcp-persist` resumability — the
`with_persistence()` plugin one-liner, plus each backend wired manually into a
real [`StreamableHTTPSessionManager`](https://github.com/modelcontextprotocol/python-sdk).

All serve MCP at `http://localhost:8000/mcp`. The three backend servers share the
same note-taking API; the plugin server is a minimal echo server.

## Prerequisites

```bash
# FastMCP plugin example (SQLite) — uvicorn ships with mcp, so one install is enough
pip install "mcp-persist[sqlite]"

# SQLite example
pip install "mcp-persist[sqlite]" uvicorn starlette

# Redis example
pip install "mcp-persist[redis]" uvicorn starlette

# Postgres example
pip install "mcp-persist[postgres]" uvicorn starlette
```

### Local Redis + Postgres

The Redis and Postgres examples need a running backend. The repo ships a
[`compose.yaml`](../compose.yaml) that starts both on their default ports:

```bash
docker compose up -d        # or: podman compose up -d
# ... run the examples ...
docker compose down         # add -v to drop the Postgres volume
```

The same services back the integration tests when you set `MCP_TEST_REDIS_URL`
and `MCP_TEST_POSTGRES_URL` (see [`compose.yaml`](../compose.yaml)).

## fastmcp_plugin_server.py

The simplest entry point: a FastMCP server made resumable with a single
`with_persistence()` call — no manual store, manager, or lifespan wiring. Uses
SQLite (persists to `echo_events.db`) and exposes `shout` / `slow_echo` tools.

```bash
python examples/fastmcp_plugin_server.py
```

## sqlite_server.py

Single-process durability with no external services. Events are persisted to
a local `notes.db` file — the server can be restarted without losing SSE
stream state.

```bash
python examples/sqlite_server.py
```

## redis_server.py

Multi-worker resumability via Redis. Any number of server processes can share
the same Redis instance; a client can reconnect to a different worker and have
missed events replayed.

Requires a running Redis instance (default: `redis://localhost:6379`).

```bash
redis-server &
python examples/redis_server.py
```

## postgres_server.py

Durable resumability for deployments already running PostgreSQL, including
multi-node / team setups. Events are persisted via an `asyncpg` connection pool.

Requires a reachable PostgreSQL instance. Set `DATABASE_URL` to override the
default (`postgresql://postgres@localhost:5432/postgres`).

```bash
python examples/postgres_server.py
```

## Trying it out

With the server running, connect any MCP client to `http://localhost:8000/mcp`.

Using the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
(the `add_note` tool below is on the three note-taking servers; on the plugin
server call `shout` instead):

```python
import asyncio
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

async def main():
    async with streamablehttp_client("http://localhost:8000/mcp") as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()

            result = await session.call_tool("add_note", {
                "title": "Hello",
                "body": "My first persisted note.",
            })
            print(result.content[0].text)

asyncio.run(main())
```
