# Examples

Three minimal MCP servers showing how to wire `mcp-persist` backends into a
real [`StreamableHTTPSessionManager`](https://github.com/modelcontextprotocol/python-sdk).

All expose the same note-taking API over MCP at `http://localhost:8000/mcp`.

## Prerequisites

```bash
# SQLite example
pip install "mcp-persist[sqlite]" uvicorn starlette

# Redis example
pip install "mcp-persist[redis]" uvicorn starlette

# Postgres example
pip install "mcp-persist[postgres]" uvicorn starlette
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

Using the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk):

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
