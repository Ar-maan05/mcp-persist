# mcp-persist

[![CI](https://github.com/Ar-maan05/mcp-persist/actions/workflows/ci.yml/badge.svg)](https://github.com/Ar-maan05/mcp-persist/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/mcp-persist.svg)](https://pypi.org/project/mcp-persist/)
[![Downloads](https://static.pepy.tech/badge/mcp-persist)](https://pepy.tech/project/mcp-persist)
[![Python versions](https://img.shields.io/pypi/pyversions/mcp-persist.svg)](https://pypi.org/project/mcp-persist/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

When an MCP client reconnects, the server has to replay the events it missed, and with only the SDK's in-memory `EventStore`, that replay is impossible: the session lived in one process's memory, so a restart or a reconnect to a different worker loses it. **`mcp-persist` adds drop-in durable `EventStore` backends for SQLite, Redis, and PostgreSQL** that survive process restarts and scale across multi-worker deployments, keeping SSE stream resumability intact.

## Two-line setup for FastMCP: `with_persistence()`

The fastest way to add resumability to a FastMCP server. Wiring it by hand means
an event store, a `StreamableHTTPSessionManager`, a Starlette lifespan to open and
close them, and a `Mount`. `with_persistence()` collapses all of it to two lines:
pass your `FastMCP` instance and get back a runnable Starlette ASGI app with the
store and session manager already wired in, opened on startup and closed on
shutdown.

**Before** (the manual wiring):

```python
import contextlib

import aiosqlite
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp_persist import SQLiteEventStore
from starlette.applications import Starlette
from starlette.routing import Mount

mcp = FastMCP(name="MyServer")


@contextlib.asynccontextmanager
async def lifespan(app):
    conn = await aiosqlite.connect("events.db")
    try:
        store = SQLiteEventStore(conn, ttl=3600)
        await store.initialize()
        manager = StreamableHTTPSessionManager(app=mcp._mcp_server, event_store=store)
        app.state.manager = manager
        async with manager.run():
            yield
    finally:
        await conn.close()


async def handle_mcp(scope, receive, send):
    await scope["app"].state.manager.handle_request(scope, receive, send)


app = Starlette(lifespan=lifespan, routes=[Mount("/mcp", app=handle_mcp)])
uvicorn.run(app, host="127.0.0.1", port=8000)
```

**After** (`with_persistence()`):

```python
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp_persist import with_persistence

mcp = FastMCP(name="MyServer")

app = with_persistence(mcp, backend="sqlite", url="events.db", ttl=3600)
uvicorn.run(app, host="127.0.0.1", port=8000)  # MCP endpoint at /mcp
```

Switching backend is a one-word change (`backend="redis"` / `"postgres"` with
the matching `url`). There are three ways to supply the store, resolved in order:

```python
# A: config kwargs; the app builds the store and owns its lifecycle:
app = with_persistence(mcp, backend="redis", url="redis://localhost:6379", ttl=3600)

# B: bring your own store; you own its lifecycle (the app does NOT close it):
async with SQLiteEventStore.create("events.db", ttl=3600) as store:
    app = with_persistence(mcp, store=store)
    await uvicorn.Server(uvicorn.Config(app, port=8000)).serve()

# C: configure from the environment (MCP_PERSIST_BACKEND / _URL / _TTL / …):
app = with_persistence(mcp)
```

The live store is exposed on `app.state.event_store`, so you can run a
[`PurgeScheduler`](#scheduled-cleanup-purgescheduler) alongside the server. No
extra dependency is required: `starlette` and the session manager ship with
`mcp`. See [`examples/fastmcp_plugin_server.py`](examples/fastmcp_plugin_server.py).

Under the hood, whichever setup you use, it's the same layering: a
`StreamableHTTPSessionManager` backed by a durable `EventStore` you choose.

```
MCP Server
     │
     ▼
StreamableHTTPSessionManager
     │
     ▼
EventStore
 ├─ SQLite
 ├─ Redis
 └─ PostgreSQL
```

## Resumability without touching the server: `PersistenceProxy`

When you can't (or don't want to) modify the MCP server, such as a third-party server,
another language, or a binary you don't own, run the **proxy** in front of it. It
forwards requests upstream and intercepts the SSE responses, persisting every
event to a store and assigning its own event IDs. A client that disconnects
reconnects with `Last-Event-ID`; the proxy replays the missed events from the
store and continues live. The upstream needs **no** event store of its own: the
proxy is the store.

> **Running a TypeScript (or any non-Python) MCP server?** The proxy speaks plain
> HTTP, so it adds resumability in front of it without touching the server. See
> [docs/typescript.md](docs/typescript.md) for a step-by-step guide.

Point your clients at the proxy's address instead of the server's (e.g.
`http://localhost:8000/mcp`); nothing else on the client changes. Resumability
rides the standard SSE `Last-Event-ID` header, so any MCP client that reconnects
after a drop gets its missed events back automatically.

```bash
# Point at a running MCP server (no extra install needed: httpx & uvicorn
# already ship with mcp):
mcp-persist-proxy --upstream http://localhost:8001 \
    --backend sqlite --url events.db --port 8000

# …or start the server as a subprocess, wait for it, and proxy it:
mcp-persist-proxy --backend redis --url redis://localhost:6379 \
    --port 8000 --upstream-port 8001 -- uvicorn my_server:app --port 8001
```

Or embed it as an ASGI app:

```python
import uvicorn
from mcp_persist import PersistenceProxy

async def serve():
    async with PersistenceProxy.create(
        "http://localhost:8001", backend="sqlite", url="events.db", ttl=3600
    ) as proxy:
        await uvicorn.Server(uvicorn.Config(proxy, port=8000)).serve()
```

The store is resolved exactly like [`with_persistence`](#two-line-setup-for-fastmcp-with_persistence):
a pre-built `store=`, `backend=`+`url=`, or `MCP_PERSIST_*` env vars. (`ttl` is
how long stored events are kept, in seconds; it's available as `--ttl` on the CLI
too.)

**What it does and does not do.** It adds resumability against a *stable
upstream*: a server that stays up while clients come and go. It survives client
disconnects (flaky networks, mobile, tunnels), and, with a durable store like
SQLite or Postgres, a restart of the proxy itself. Two things it can't do: it
can't recover from the **upstream server** restarting: a restarted server is a
clean break, so the proxy can replay what it already stored but can't carry the
old connection over to the new server; and it can't replay an event that was
never stored: if the client and the proxy both drop before an event is saved,
it's gone. It never makes delivery less reliable than talking to the server
directly.

## Diagnostics: `mcp-persist doctor`

Before you debug a deployment, run the doctor. It is a pass/fail checklist for the
things that usually explain a broken or silently growing store: the Python
runtime, whether the backend's driver extra is installed, live connectivity, and
config that lets events accumulate without bound.

```bash
# Check a specific store:
mcp-persist doctor --backend sqlite --url events.db --ttl 3600

# …or check whatever MCP_PERSIST_* is configured (no flags needed):
mcp-persist doctor

# Machine-readable, for CI or a readiness gate:
mcp-persist doctor --json
```

```text
mcp-persist doctor: redis (redis://localhost:6379)

[ ok ] python        Python 3.12.13 (>= 3.10)
[ ok ] driver        redis is installed for the redis backend
[ ok ] connectivity  connected to redis (redis 7.2.0)
[warn] retention     ttl is not set: events accumulate in Redis indefinitely; set --ttl

All checks passed with 1 warning(s).
```

The runtime, driver, and retention checks read your resolved config, so they run
even when the backend is unreachable (exactly when you reach for the doctor); a
store that will not open is reported as a failed `connectivity` check rather than
a crash. The command exits non-zero only when a check **fails**; warnings (an
unset `ttl`, for example) are surfaced but do not fail the run, so a warning will
not break a CI gate that treats exit code as health.

## Inspecting streams: `mcp-persist stats`

`mcp-persist stats` reports how many events each stream holds, their event ID
range, and a latency probe timed against the backend's native `PING` / `SELECT 1`.
It reads the store directly (a single `ZCARD`/`ZRANGE` pass on Redis, one
`GROUP BY stream_id` on SQLite/Postgres), so it is cheap to run against a live
deployment.

```bash
# Every stream, plus totals and a latency probe:
mcp-persist stats --backend sqlite --url events.db

# A single stream:
mcp-persist stats --backend redis --url redis://localhost:6379 --stream-id session-42:_GET_stream

# JSON for scripting / dashboards:
mcp-persist stats --json
```

```text
mcp-persist stats: sqlite (events.db)

stream                   events  min  max
session-a:_GET_stream        12    1   12
session-b:notifications       5   13   17

2 stream(s), 17 event(s), last id 17, ping 0.11 ms
```

`last id` is the latest event ID assigned: the never-expired counter on Redis, or
the highest stored ID on SQLite/Postgres (which can trail the sequence once old
rows are purged). Config is resolved exactly like the proxy and `doctor`
(`--backend`/`--url` or `MCP_PERSIST_*`). An unreachable store prints a single
error line and exits non-zero rather than a traceback.

## Backends

| Backend | Extra | Use case |
|---|---|---|
| `SQLiteEventStore` | `sqlite` | Single-process SSE resumability across restarts, with no external service |
| `RedisEventStore` | `redis` | Multi-process / multi-worker SSE resumability |
| `PostgresEventStore` | `postgres` | Durable resumability for deployments already running Postgres, including multi-node / team setups |

## Choosing a backend

Start from how you deploy:

| If your deployment… | Use |
|---|---|
| Runs as a single process and you want zero extra infrastructure | `SQLiteEventStore` |
| Runs multiple workers / replicas behind a load balancer | `RedisEventStore` |
| Already runs PostgreSQL, or needs durable storage at team / multi-node scale | `PostgresEventStore` |
| Runs on serverless / a read-only or ephemeral filesystem | `RedisEventStore` or `PostgresEventStore` (never SQLite) |

> **Any replica count > 1 needs a *shared* store (Redis/Postgres), not SQLite.**
> A local SQLite file is visible only to the process that opened it, so behind a
> load balancer (or during a rolling deploy, when a reconnecting client lands on
> a different pod) that pod won't have the client's events and the resume
> silently returns nothing. SQLite is for a genuine single process. See
> [deployment topologies](docs/production.md#deployment-topologies-rolling-deploys-load-balancers-serverless).

How they compare:

| | SQLite | Redis | Postgres |
|---|---|---|---|
| External service | None | Redis | PostgreSQL |
| Multi-process / multi-worker | No (single writer) | Yes | Yes |
| Durable across restarts | Yes (on disk) | Depends on Redis persistence config | Yes |
| Automatic expiry | No (call `purge_expired()`) | Yes (native key TTL) | No (call `purge_expired()`) |
| Best fit | Single node, edge, local dev | Load-balanced / ephemeral fan-out | Teams already running Postgres |

See [Benchmarks](#benchmarks) for latency and throughput characteristics.

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

On FastMCP, one line wires a durable store into a runnable app; see
[Two-line setup](#two-line-setup-for-fastmcp-with_persistence) for the full
before/after:

```python
from mcp.server.fastmcp import FastMCP
from mcp_persist import with_persistence

mcp = FastMCP(name="MyServer")

# Swap backend="redis" / "postgres" with the matching url:
app = with_persistence(mcp, backend="sqlite", url="events.db", ttl=3600)
# run it:  uvicorn.run(app, host="127.0.0.1", port=8000)   # MCP endpoint at /mcp
```

Not on FastMCP, or want to own the wiring yourself? Build a store and pass it to
`StreamableHTTPSessionManager` directly; see
[Manual wiring](#manual-wiring-advanced-or-non-fastmcp) below.

## Manual wiring (advanced or non-FastMCP)

Construct a store and hand it to `StreamableHTTPSessionManager`. The backends are
interchangeable; pick per [Choosing a backend](#choosing-a-backend).

### SQLite

```python
import aiosqlite
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp_persist import SQLiteEventStore

mcp = FastMCP(name="MyServer")

conn = await aiosqlite.connect("events.db")
store = SQLiteEventStore(conn, ttl=3600)  # 1 hour TTL
await store.initialize()

session_manager = StreamableHTTPSessionManager(
    app=mcp._mcp_server,  # the low-level Server that FastMCP wraps
    event_store=store,
)
```

### Redis

```python
import redis.asyncio as aioredis
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp_persist import RedisEventStore

mcp = FastMCP(name="MyServer")

redis_client = aioredis.from_url("redis://localhost:6379")
store = RedisEventStore(redis_client, ttl=3600)  # 1 hour TTL

session_manager = StreamableHTTPSessionManager(
    app=mcp._mcp_server,  # the low-level Server that FastMCP wraps
    event_store=store,
)
```

### Postgres

```python
import asyncpg
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp_persist import PostgresEventStore

mcp = FastMCP(name="MyServer")

pool = await asyncpg.create_pool("postgresql://localhost/mydb")
store = PostgresEventStore(pool, ttl=3600)  # 1 hour TTL
await store.initialize()

session_manager = StreamableHTTPSessionManager(
    app=mcp._mcp_server,  # the low-level Server that FastMCP wraps
    event_store=store,
)
```

## SQLiteEventStore

Stores MCP SSE events in a SQLite database so a single-process server can resume
interrupted streams across restarts and redeploys, without running Redis or any
other external service. Ideal for single-node deployments, local development, and
edge/embedded hosts.

> For load-balanced or multi-worker deployments, use `RedisEventStore` instead:
> SQLite is single-writer and not designed for shared multi-process access.
> Multiple processes writing the same database file contend on SQLite's file
> lock and will surface `SQLITE_BUSY` / "database is locked" errors.

### How it works

One row per event:

```
{table}.event_id    - INTEGER PRIMARY KEY AUTOINCREMENT, monotonic event IDs (never reused)
{table}.stream_id   - TEXT, the stream the event belongs to
{table}.payload     - TEXT, serialized JSONRPCMessage ("" for priming events)
{table}.created_at  - REAL, unix timestamp used for TTL expiry
```

- **Monotonic IDs** via `AUTOINCREMENT`: strictly increasing, never reused, same guarantee as Redis `INCR`
- **Indexed replay**: `WHERE stream_id = ? AND event_id > ?` over a `(stream_id, event_id)` index
- **Durable across restarts**: WAL journaling; events survive process exit
- **TTL support**: expired events are skipped on replay and removed by `purge_expired()`
- **Multi-tenant isolation** via configurable `table_name`
- **Priming event handling**: sentinel empty-string payloads are stored but never replayed
- **Optional write-behind**: `commit_interval` batches commits for higher write throughput at a bounded durability window (see below)

### Configuration

```python
SQLiteEventStore(
    conn,                   # an open aiosqlite.Connection
    table_name="mcp_events",  # isolate multiple servers in one database file
    ttl=3600,               # seconds; None = never expire (not recommended)
    compression=None,       # "gzip" to compress large payloads (see "Large payloads")
    commit_interval=None,   # seconds; set to batch commits (write-behind, see below)
    commit_max_pending=None,  # cap buffered events under write-behind
)
```

**TTL note:** SQLite has no automatic key expiry. Events past `ttl` are skipped on
replay, but to reclaim disk space call `await store.purge_expired()` periodically
(e.g. from a background task). It returns the number of rows deleted.

### Write-behind commits

By default every `store_event` commits (one `fsync` each): durable, but the
throughput ceiling. Set `commit_interval` (seconds) to commit on a background
timer instead, and optionally `commit_max_pending` to also commit once that many
events are buffered:

```python
async with SQLiteEventStore.create("events.db", ttl=3600, commit_interval=1.0) as store:
    ...  # commits at most once a second; far higher write throughput
```

Buffered events are still immediately visible to replay/subscribe on the same
store; the trade-off is that a crash loses up to one `commit_interval` of
uncommitted events. **You must close the store** so the last batch is flushed:
`create()` and `async with store:` do this for you, or call `await store.aclose()`
on shutdown. Off by default. See
[docs/production.md](docs/production.md#12-write-behind-commits-sqlite) for the
full trade-off and single-writer caveat.

### Multi-tenant deployments

If multiple MCP servers share a database file, use different table names:

```python
store_a = SQLiteEventStore(conn, table_name="server_a")
store_b = SQLiteEventStore(conn, table_name="server_b")
```

## RedisEventStore

Stores MCP SSE events in Redis so clients can resume interrupted streams, even across worker restarts or load-balanced deployments.

### How it works

Redis data layout:

```
{prefix}counter                 - atomic INCR source for monotonic event IDs (never expires)
{prefix}event:{event_id}        - HASH: stream_id + serialized payload
{prefix}stream:{stream_id}      - ZSET: event IDs sorted by score for O(log N) range queries
```

- **Atomic monotonic IDs** via Redis `INCR`: collision-free across concurrent workers. The counter is never given a TTL (even when `ttl` is set), so IDs stay monotonic across idle periods; only the event and stream keys expire.
- **Replay is O(log N + M)**: one `ZRANGEBYSCORE` range-scans the stream's sorted set, then each of the M matched events is fetched with its own `HGET`. That's one network round-trip per replayed event: fine for typical resume sizes, worth knowing for very long streams.
- **TTL support**: automatic expiry of event/stream keys to prevent unbounded memory growth
- **Atomic writes**: each event's hash, sorted-set entry, and TTLs are written in a single transactional pipeline, so a mid-write crash can't orphan a hash or leave a key without its expiry
- **Multi-tenant isolation** via configurable `key_prefix`
- **Priming event handling**: sentinel empty-string payloads are stored but never replayed to clients

### Configuration

```python
RedisEventStore(
    redis,                  # redis.asyncio.Redis instance
    key_prefix="mcp:",      # isolate multiple servers on one Redis instance
    ttl=3600,               # seconds; None = never expire (not recommended)
    max_stream_length=None, # optional cap on how many event IDs each stream retains
    compression=None,       # "gzip" to compress large payloads (see "Large payloads")
)
```

- **TTL guidance:** Set `ttl` to at least 2× your session idle timeout. If you leave it as `None`, a warning is logged and events accumulate indefinitely.
- **Stream bounds (`max_stream_length`):** Set a positive integer to cap the size of each stream's sorted set. The oldest event IDs beyond this limit are automatically trimmed on every write, preventing unbounded memory growth on long-lived streams.

#### Production Note: Stream Cardinality & Redis Memory Growth

When scaling to millions of unique stream IDs, Redis accumulates:
- One `{prefix}event:{event_id}` HASH key per event.
- One `{prefix}stream:{stream_id}` ZSET key per stream.
- A global `{prefix}counter` string (never expires, preserving ID monotonicity).

While event hashes and stream ZSETs expire automatically when `ttl` is set, a massive rate of unique stream creation (e.g., one-off clients) can accumulate many ZSET keys in memory within the TTL window.

**Best Practices:**
1. **Always configure a TTL** to ensure inactive streams and their events are automatically evicted.
2. **Use a Volatile Eviction Policy:** Configure Redis with `volatile-lru` or `volatile-ttl`. **Do not use `allkeys-lru`**, as this can evict the global `{prefix}counter` key. If the counter key is evicted, the ID sequence resets, breaking stream resumability guarantees.
3. **Limit Stream Cardinality** at the application level if possible by grouping related connections.

### Multi-tenant deployments

If multiple MCP servers share a Redis instance, use different prefixes:

```python
store_a = RedisEventStore(redis_client, key_prefix="server-a:")
store_b = RedisEventStore(redis_client, key_prefix="server-b:")
```

## PostgresEventStore

Stores MCP SSE events in PostgreSQL so servers can resume interrupted streams
across restarts and redeploys. It takes an `asyncpg` connection pool, so
concurrent request handlers share connections cleanly, a good fit for
deployments that already run Postgres and want durability without adding Redis.

> For ephemeral multi-worker fan-out, `RedisEventStore` is lighter; for a pure
> single-process server with no external service, use `SQLiteEventStore`.

### How it works

One row per event:

```
{table}.event_id    - BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, monotonic IDs (never reused)
{table}.stream_id   - TEXT, the stream the event belongs to
{table}.payload     - TEXT, serialized JSONRPCMessage ("" for priming events)
{table}.created_at  - DOUBLE PRECISION, unix timestamp used for TTL expiry
```

- **Monotonic IDs** via an `IDENTITY` column: strictly increasing, never reused, same guarantee as Redis `INCR`
- **Indexed replay**: `WHERE stream_id = $1 AND event_id > $2` over a `(stream_id, event_id)` index
- **Pooled & concurrent**: accepts an `asyncpg.Pool`, so many handlers can store/replay without contending on one connection
- **TTL support**: expired events are skipped on replay and removed by `purge_expired()`
- **Multi-tenant isolation** via configurable `table_name`
- **Priming event handling**: sentinel empty-string payloads are stored but never replayed

### Configuration

```python
PostgresEventStore(
    pool,                     # an asyncpg.Pool
    table_name="mcp_events",  # isolate multiple servers in one database
    ttl=3600,                 # seconds; None = never expire (not recommended)
    replay_batch_size=500,    # rows fetched per round-trip on replay; lower for very large payloads
    compression=None,         # "gzip" to compress large payloads (see "Large payloads")
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

## Connection lifecycle: `create()`

Each store also accepts a connection it owns directly, opened and closed for you.
`SQLiteEventStore.create()`, `RedisEventStore.create()`, and
`PostgresEventStore.create()` are async context managers that take a connection
string, build the underlying driver client (and call `initialize()` where
needed), yield a ready store, and close the connection on exit, including when
the body raises:

```python
from mcp_persist import SQLiteEventStore, RedisEventStore, PostgresEventStore

async with SQLiteEventStore.create("events.db", ttl=3600) as store:
    await store.store_event(...)

async with RedisEventStore.create("redis://localhost:6379", ttl=3600) as store:
    ...

async with PostgresEventStore.create("postgresql://localhost/mydb", ttl=3600) as store:
    ...
```

Extra keyword arguments are forwarded to the driver (`aiosqlite.connect`,
`redis.asyncio.from_url`, `asyncpg.create_pool`). To share one client/pool across
stores or manage its lifecycle yourself, construct the store directly as shown in
[Quickstart](#quickstart).

## Real-time streaming: `subscribe()`

Beyond SSE resumability, each store can push new events to an in-process consumer
as they are written. Pass `enable_streaming=True`, then iterate `subscribe()`:

```python
store = RedisEventStore(redis_client, ttl=3600, enable_streaming=True)

async for event_id, message in store.subscribe("stream-id"):
    handle(message)
```

- **Forward-only:** a subscriber receives only events written *after* it
  subscribes (use `replay_events_after` for backfill). Priming events are skipped.
- **Per backend:** Redis uses pub/sub and Postgres uses `LISTEN`/`NOTIFY`, so
  delivery is push-based; SQLite has no native notification and falls back to
  polling (`subscribe(stream_id, poll_interval=0.5)`).
- **Connection cost:** Redis and Postgres subscribers each hold a dedicated
  connection for their lifetime; size your pool for the expected number of
  concurrent subscribers. See [docs/production.md](docs/production.md) for sizing.

## Cross-backend migration: `migrate()`

Copy events from one store to another, e.g. SQLite → Postgres as a single-node
deployment grows, or Redis → Postgres for durability:

```python
from mcp_persist import migrate

result = await migrate(sqlite_store, postgres_store)
print(result.events_migrated, result.failed_streams)
```

It streams events from the source (oldest first) and re-stores them on the
destination, preserving per-stream ordering. Each stream migrates independently:
a failing stream is logged and recorded in `failed_streams`, not fatal. Pass
`stream_id=` to migrate one stream, `batch_size=` and `on_progress=` to drive a
progress bar.

> **Caveats (read before migrating production data):** event IDs are *not*
> preserved (the destination assigns fresh ones), timestamps reset (TTL clock
> restarts), and resumability tokens are therefore invalidated: clients holding
> a `Last-Event-ID` from the source cannot resume against the destination. The
> copy is also not consistent under concurrent writes, so stop writes to the
> source first. See [docs/production.md](docs/production.md) for the full
> migration runbook.

## Metrics & observability

Every store accepts an optional `metrics=` collector that fires on each store,
replay, and error. The default is a no-op the stores special-case to zero
overhead, so you pay nothing unless you opt in:

```python
from mcp_persist import RedisEventStore, LoggingMetricsCollector

# Batteries-included: logs one line per operation at DEBUG.
store = RedisEventStore(redis_client, ttl=3600, metrics=LoggingMetricsCollector())
```

To emit to Prometheus, Datadog, or anything else, pass any object with three
synchronous methods: `on_store_event(stream_id, event_id, duration_ms)`,
`on_replay(stream_id, events_replayed, duration_ms)`, and
`on_error(operation, error)`. It does not need to subclass `MetricsCollector`
(it's a `Protocol`). A collector that raises is logged and ignored rather than
allowed to fail the underlying operation.

## Large payloads: `compression`

When MCP messages carry large tool results or big JSON-RPC bodies, pass
`compression="gzip"` to gzip-compress payloads above `compress_min_bytes`
(default `1024`) before they are stored, cutting storage and, on Redis, memory:

```python
store = PostgresEventStore(pool, ttl=3600, compression="gzip", compress_min_bytes=1024)
```

Decompression on read is automatic and **independent of the setting**: a store
with compression off still reads payloads written compressed, so you can enable
it on a rolling deploy and `migrate()` across stores with mismatched settings.
Small or incompressible payloads are stored plain (never made larger), and
existing data is unaffected. Available on all three backends.

## Scheduled cleanup: `PurgeScheduler`

SQLite and Postgres need `purge_expired()` called periodically (Redis expires
keys natively). `PurgeScheduler` runs it for you on an interval:

```python
from mcp_persist import PurgeScheduler

async with PurgeScheduler(store, interval=300, batch_size=1000):
    async with manager.run():
        yield
```

It logs `purged N events`, survives transient backend errors, and rejects a
`RedisEventStore` (which has nothing to purge). `batch_size` is optional and
forwards to `purge_expired(batch_size=...)` so a large purge deletes in bounded
chunks instead of one long-locking `DELETE`. Use `start()` / `aclose()` if you
prefer explicit lifecycle management over a `with` block.

## Configuration from the environment: `event_store_from_env()`

Pick the backend at deploy time without branching in code:

```python
from mcp_persist import event_store_from_env

# MCP_PERSIST_BACKEND=redis  MCP_PERSIST_URL=redis://localhost:6379  MCP_PERSIST_TTL=3600
async with event_store_from_env() as store:
    ...
```

Reads `MCP_PERSIST_BACKEND` (`sqlite`/`redis`/`postgres`) and `MCP_PERSIST_URL`,
plus optional `MCP_PERSIST_TTL`, `MCP_PERSIST_TABLE_NAME` (SQLite/Postgres), and
`MCP_PERSIST_KEY_PREFIX` / `MCP_PERSIST_MAX_STREAM_LENGTH` (Redis). Returns the
chosen backend's `create()` context manager, so the connection is opened on entry
and closed on exit.

## Readiness probes: `ping()`

Every store exposes `await store.ping()` (Redis `PING`, Postgres/SQLite
`SELECT 1`) for liveness/readiness checks. It returns `True` when the backend is
reachable and lets the driver error propagate otherwise, so a health endpoint can
report "not ready" when the store's dependency is down. See
[docs/production.md](docs/production.md#11-readiness-probes-ping).

## Examples

The [`examples/`](examples/) directory contains minimal, runnable MCP servers:
the `with_persistence()` one-liner, plus each backend wired manually into a real
[`StreamableHTTPSessionManager`](https://github.com/modelcontextprotocol/python-sdk):

| File | Approach | Run |
|---|---|---|
| [`fastmcp_plugin_server.py`](examples/fastmcp_plugin_server.py) | `with_persistence()` plugin (SQLite) | `python examples/fastmcp_plugin_server.py` |
| [`sqlite_server.py`](examples/sqlite_server.py) | Manual `SQLiteEventStore` | `python examples/sqlite_server.py` |
| [`redis_server.py`](examples/redis_server.py) | Manual `RedisEventStore` | `python examples/redis_server.py` |
| [`postgres_server.py`](examples/postgres_server.py) | Manual `PostgresEventStore` | `python examples/postgres_server.py` |

Each one is a self-contained MCP server you can connect to with any MCP client at
`http://localhost:8000/mcp` (the three backend servers are a note-taking app; the
plugin server is a minimal echo server).

See [`examples/README.md`](examples/README.md) for prerequisites, setup, and a
client snippet.

## Benchmarks

[`benchmarks/benchmark.py`](benchmarks/benchmark.py) measures `store_event`
latency (sequential), `store_event` throughput (concurrent), and
`replay_events_after` latency across all three backends. SQLite runs against an
on-disk file (its realistic durable mode), and Redis/Postgres run over the
network. Run it yourself:

```bash
uv run python benchmarks/benchmark.py --events 5000 --concurrency 500
```

> **These numbers are indicative, not authoritative.** Absolute latency and
> throughput depend heavily on hardware, disk, network, and server tuning. Run the script in *your* environment for numbers that matter.

### Benchmark Environment Spec
The table below was measured with the following configuration:
- **CPU / Machine:** AMD Ryzen AI 7 350 (8 cores, 16 threads), 24GB DDR5 5600, PCIe Gen 5 NVMe SSD storage, running Fedora Linux 44 (Workstation Edition) x86_64
- **Python Version:** 3.12.2
- **Redis Version:** 8.8.0 (container on localhost)
- **PostgreSQL Version:** 18.4 (container on localhost)

Measured with `--events 5000 --concurrency 500`:

#### Storage Performance

| Backend | store p50 | store p95 | store mean | store throughput |
|---|---|---|---|---|
| SQLite | 57.2 µs | 78.4 µs | 61.6 µs | 23,517 ev/s |
| Redis | 65.6 µs | 93.1 µs | 73.7 µs | 7,857 ev/s |
| Postgres | 626.1 µs | 913.4 µs | 660.0 µs | 7,427 ev/s |

#### Replay Performance (Total Latency)

| Backend | Replay 100 | Replay 1,000 | Replay 10,000 |
|---|---|---|---|
| SQLite | 0.93 ms | 6.51 ms | 27.41 ms |
| Redis | 1.00 ms | 8.79 ms | 76.08 ms |
| Postgres | 2.96 ms | 6.58 ms | 61.13 ms |

What the shape of these results reflects (and should hold across environments):

- **SQLite has the lowest latency _and_ the highest throughput**: it runs
  in-process with no network hop, so every `store_event` skips a round-trip
  entirely. The catch is that it's single-writer: that throughput doesn't scale
  across processes, which is why multi-worker deployments still reach for Redis
  or Postgres despite the lower single-node numbers.
- **Redis and Postgres pay a network round-trip per store**, so per-call latency
  is higher than SQLite. The two land at comparable throughput (~7,400–7,900
  ev/s at concurrency 500) for opposite reasons: Redis has low per-call latency
  but every write serializes through the single `INCR` counter (see the write
  ceiling note below), while Postgres has much higher per-call latency but its
  pooled connections run many stores concurrently.
- **Replay**: SQLite and Postgres fetch a stream's events in one indexed query, while the Redis backend issues a `zrangebyscore` followed by a single pipelined execution to fetch payloads concurrently, keeping the entire replay latency bounded to exactly two network round-trips.

## Architecture & Guarantees

This section outlines the consistency, ordering, and concurrency guarantees of `mcp-persist` backends.

### 1. Event Ordering
- **Per-Stream vs Global:** All backends guarantee that event IDs are monotonically increasing, representing a sequential log of events. However, because client-side replay request handling relies on range scans queryable by stream ID, ordering guarantees are **per-stream**.
- **Preserved Order:** Outbound events written to a specific stream via `store_event` are guaranteed to be replayed in the exact order they were written.

### 2. Concurrency & Write Semantics
- **Concurrent Writes:**
  - **Redis:** `store_event` increments a global atomic counter via `INCR` to get the next sequential ID, and then pipelined commands write the event hash and add it to the stream's sorted set. Multiple workers can write concurrently without any locking, and the IDs are guaranteed to be unique and monotonically increasing.
  - **SQLite:** SQLite is single-writer and serializes all writes. `aiosqlite` uses an in-process thread pool to queue commands on a single connection. Concurrent writes from multiple processes are not supported and will raise `SQLITE_BUSY` errors.
  - **PostgreSQL:** Uses a native `BIGINT GENERATED ALWAYS AS IDENTITY` column which handles concurrent sequence increments safely across database sessions.
- **Duplicate Event IDs:** Duplicate event IDs are structurally impossible. All backends rely on atomic database counters (`AUTOINCREMENT` for SQLite, `IDENTITY` for Postgres, and `INCR` for Redis) which generate strictly unique and non-overlapping sequence numbers.
- **Redis counter as the write ceiling:** Because every write `INCR`s a single `{prefix}counter` key, all writes serialize through that one key. On a single Redis it is rarely the bottleneck, but on **Redis Cluster** the counter lives on one shard, setting the aggregate write-throughput ceiling regardless of cluster size. See [docs/production.md](docs/production.md#redis-monotonic-counter-throughput-ceiling) for benchmarking guidance.

### 3. Consistency & Durability
- **SQLite:** Configured with WAL (`Write-Ahead Logging`) journaling. Writes are flushed to disk on commit, ensuring durability across process restarts.
- **Postgres:** Fully ACID compliant. Events are durable once the transaction commits.
- **Redis:** Relies on Redis persistence configuration (RDB/AOF). If Redis is deployed as a cache (with no persistence) or with lazy AOF flushing, a Redis crash could roll back the database state, potentially repeating or dropping IDs. For strong durability, configure Redis with AOF (`appendfsync everysec` or `always`).

## Deploying to production

Once a backend is wired in, see the **[production guide](docs/production.md)** for
operating it: scheduling `purge_expired()` so storage doesn't grow without bound,
treating the store as a critical dependency (failure modes), pre-creating schema
under restricted database permissions, TLS and credential handling, connection
and pool sizing across workers, and a deployment checklist.

## Development

```bash
git clone https://github.com/Ar-maan05/mcp-persist
cd mcp-persist
uv sync --all-extras --dev
uv run pytest tests/
```

The suite is 300+ async tests covering all three backends. The Redis tests use
[fakeredis](https://github.com/cunla/fakeredis-py) and the SQLite tests use
in-memory `aiosqlite`, so the default run needs no external servers. The Postgres tests require a real server and are skipped unless
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
