# mcp-persist

[![CI](https://github.com/Ar-maan05/mcp-persist/actions/workflows/ci.yml/badge.svg)](https://github.com/Ar-maan05/mcp-persist/actions/workflows/ci.yml)

Production-grade persistence backends for the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk).

The MCP SDK ships an `EventStore` interface but only an in-memory reference implementation. `mcp-persist` provides backends for real deployments where you need durability across process restarts and multi-worker environments.

## Backends

| Backend | Extra | Use case |
|---|---|---|
| `SQLiteEventStore` | `sqlite` | Single-process SSE resumability across restarts, with no external service |
| `RedisEventStore` | `redis` | Multi-process / multi-worker SSE resumability |
| `PostgresEventStore` | `postgres` | Durable resumability for deployments already running Postgres, including multi-node / team setups |

## Installation

```bash
# SQLite backend (no external service needed)
pip install "mcp-persist[sqlite]"

# Redis backend
pip install "mcp-persist[redis]"

# Postgres backend
pip install "mcp-persist[postgres]"

# Multiple backends
pip install "mcp-persist[sqlite,redis,postgres]"
```

## Quickstart

### SQLite

```python
import aiosqlite
from mcp_persist import SQLiteEventStore
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

conn = await aiosqlite.connect("events.db")
store = SQLiteEventStore(conn, ttl=3600)  # 1 hour TTL
await store.initialize()

session_manager = StreamableHTTPSessionManager(
    app=mcp_server,
    event_store=store,
)
```

### Redis

```python
import redis.asyncio as aioredis
from mcp_persist import RedisEventStore
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

redis_client = aioredis.from_url("redis://localhost:6379")
store = RedisEventStore(redis_client, ttl=3600)  # 1 hour TTL

session_manager = StreamableHTTPSessionManager(
    app=mcp_server,
    event_store=store,
)
```

### Postgres

```python
import asyncpg
from mcp_persist import PostgresEventStore
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

pool = await asyncpg.create_pool("postgresql://localhost/mydb")
store = PostgresEventStore(pool, ttl=3600)  # 1 hour TTL
await store.initialize()

session_manager = StreamableHTTPSessionManager(
    app=mcp_server,
    event_store=store,
)
```

## SQLiteEventStore

Stores MCP SSE events in a SQLite database so a single-process server can resume
interrupted streams across restarts and redeploys — without running Redis or any
other external service. Ideal for single-node deployments, local development, and
edge/embedded hosts.

> For load-balanced or multi-worker deployments, use `RedisEventStore` instead —
> SQLite is single-writer and not designed for shared multi-process access.

### How it works

One row per event:

```
{table}.event_id    — INTEGER PRIMARY KEY AUTOINCREMENT, monotonic event IDs (never reused)
{table}.stream_id   — TEXT, the stream the event belongs to
{table}.payload     — TEXT, serialized JSONRPCMessage ("" for priming events)
{table}.created_at  — REAL, unix timestamp used for TTL expiry
```

- **Monotonic IDs** via `AUTOINCREMENT` — strictly increasing, never reused, same guarantee as Redis `INCR`
- **Indexed replay** — `WHERE stream_id = ? AND event_id > ?` over a `(stream_id, event_id)` index
- **Durable across restarts** — WAL journaling; events survive process exit
- **TTL support** — expired events are skipped on replay and removed by `purge_expired()`
- **Multi-tenant isolation** via configurable `table_name`
- **Priming event handling** — sentinel empty-string payloads are stored but never replayed

### Configuration

```python
SQLiteEventStore(
    conn,                   # an open aiosqlite.Connection
    table_name="mcp_events",  # isolate multiple servers in one database file
    ttl=3600,               # seconds; None = never expire (not recommended)
)
```

**TTL note:** SQLite has no automatic key expiry. Events past `ttl` are skipped on
replay, but to reclaim disk space call `await store.purge_expired()` periodically
(e.g. from a background task). It returns the number of rows deleted.

### Multi-tenant deployments

If multiple MCP servers share a database file, use different table names:

```python
store_a = SQLiteEventStore(conn, table_name="server_a")
store_b = SQLiteEventStore(conn, table_name="server_b")
```

## RedisEventStore

Stores MCP SSE events in Redis so clients can resume interrupted streams — even across worker restarts or load-balanced deployments.

### How it works

Redis data layout:

```
{prefix}counter                 — atomic INCR source for monotonic event IDs
{prefix}event:{event_id}        — HASH: stream_id + serialized payload
{prefix}stream:{stream_id}      — ZSET: event IDs sorted by score for O(log N) range queries
```

- **Atomic monotonic IDs** via Redis `INCR` — collision-free across concurrent workers
- **O(log N) replay** via sorted set `ZRANGEBYSCORE`
- **TTL support** — automatic key expiry to prevent unbounded memory growth
- **Multi-tenant isolation** via configurable `key_prefix`
- **Priming event handling** — sentinel empty-string payloads are stored but never replayed to clients

### Configuration

```python
RedisEventStore(
    redis,                  # redis.asyncio.Redis instance
    key_prefix="mcp:",      # isolate multiple servers on one Redis instance
    ttl=3600,               # seconds; None = never expire (not recommended)
)
```

**TTL guidance:** Set `ttl` to at least 2× your session idle timeout. If you leave it as `None`, a warning is logged and events accumulate indefinitely.

### Multi-tenant deployments

If multiple MCP servers share a Redis instance, use different prefixes:

```python
store_a = RedisEventStore(redis_client, key_prefix="server-a:")
store_b = RedisEventStore(redis_client, key_prefix="server-b:")
```

## PostgresEventStore

Stores MCP SSE events in PostgreSQL so servers can resume interrupted streams
across restarts and redeploys. It takes an `asyncpg` connection pool, so
concurrent request handlers share connections cleanly — a good fit for
deployments that already run Postgres and want durability without adding Redis.

> For ephemeral multi-worker fan-out, `RedisEventStore` is lighter; for a pure
> single-process server with no external service, use `SQLiteEventStore`.

### How it works

One row per event:

```
{table}.event_id    — BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, monotonic IDs (never reused)
{table}.stream_id   — TEXT, the stream the event belongs to
{table}.payload     — TEXT, serialized JSONRPCMessage ("" for priming events)
{table}.created_at  — DOUBLE PRECISION, unix timestamp used for TTL expiry
```

- **Monotonic IDs** via an `IDENTITY` column — strictly increasing, never reused, same guarantee as Redis `INCR`
- **Indexed replay** — `WHERE stream_id = $1 AND event_id > $2` over a `(stream_id, event_id)` index
- **Pooled & concurrent** — accepts an `asyncpg.Pool`, so many handlers can store/replay without contending on one connection
- **TTL support** — expired events are skipped on replay and removed by `purge_expired()`
- **Multi-tenant isolation** via configurable `table_name`
- **Priming event handling** — sentinel empty-string payloads are stored but never replayed

### Configuration

```python
PostgresEventStore(
    pool,                     # an asyncpg.Pool
    table_name="mcp_events",  # isolate multiple servers in one database
    ttl=3600,                 # seconds; None = never expire (not recommended)
)
```

**TTL note:** PostgreSQL has no automatic row expiry. Events past `ttl` are
skipped on replay, but to reclaim space call `await store.purge_expired()`
periodically (e.g. from a background task or `pg_cron`). It returns the number
of rows deleted.

### Multi-tenant deployments

If multiple MCP servers share a database, use different table names:

```python
store_a = PostgresEventStore(pool, table_name="server_a")
store_b = PostgresEventStore(pool, table_name="server_b")
```

## Examples

The [`examples/`](examples/) directory contains minimal, runnable MCP servers
that wire each backend into a real
[`StreamableHTTPSessionManager`](https://github.com/modelcontextprotocol/python-sdk):

| File | Backend | Run |
|---|---|---|
| [`sqlite_server.py`](examples/sqlite_server.py) | `SQLiteEventStore` | `python examples/sqlite_server.py` |
| [`redis_server.py`](examples/redis_server.py) | `RedisEventStore` | `python examples/redis_server.py` |
| [`postgres_server.py`](examples/postgres_server.py) | `PostgresEventStore` | `python examples/postgres_server.py` |

Each example is a self-contained note-taking MCP server (tools, resources) that
you can connect to with any MCP client at `http://localhost:8000/mcp`.

See [`examples/README.md`](examples/README.md) for prerequisites, setup, and a
client snippet.

## Development

```bash
git clone https://github.com/Ar-maan05/mcp-persist
cd mcp-persist
uv sync --all-extras --dev
uv run pytest tests/
```

The Redis tests use [fakeredis](https://github.com/cunla/fakeredis-py) and the
SQLite tests use in-memory `aiosqlite`, so the default run needs no external
servers. The Postgres tests require a real server and are skipped unless
`MCP_TEST_POSTGRES_URL` is set; to run them and the Redis suite against real
backends:

```bash
MCP_TEST_REDIS_URL=redis://localhost:6379/0 \
MCP_TEST_POSTGRES_URL=postgresql://postgres@localhost:5432/postgres \
uv run pytest tests/
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for more.

## License

MIT
