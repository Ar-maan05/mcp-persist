# mcp-persist

[![CI](https://github.com/Ar-maan05/mcp-persist/actions/workflows/ci.yml/badge.svg)](https://github.com/Ar-maan05/mcp-persist/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/mcp-persist.svg)](https://pypi.org/project/mcp-persist/)
[![Python versions](https://img.shields.io/pypi/pyversions/mcp-persist.svg)](https://pypi.org/project/mcp-persist/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

The MCP Python SDK currently provides only an in-memory `EventStore`. **`mcp-persist` provides drop-in durable `EventStore` implementations for SQLite, Redis, and PostgreSQL.**

This allows real deployments to survive process restarts and scale across multi-worker environments while retaining SSE stream resumability.

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
> load balancer — or during a rolling deploy, when a reconnecting client lands on
> a different pod — that pod won't have the client's events and the resume
> silently returns nothing. SQLite is for a genuine single process. See
> [deployment topologies](docs/production.md#deployment-topologies-rolling-deploys-load-balancers-serverless).

How they compare:

| | SQLite | Redis | Postgres |
|---|---|---|---|
| External service | None | Redis | PostgreSQL |
| Multi-process / multi-worker | No (single writer) | Yes | Yes |
| Durable across restarts | Yes (on disk) | Depends on Redis persistence config | Yes |
| Automatic expiry | No — call `purge_expired()` | Yes (native key TTL) | No — call `purge_expired()` |
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
interrupted streams across restarts and redeploys — without running Redis or any
other external service. Ideal for single-node deployments, local development, and
edge/embedded hosts.

> For load-balanced or multi-worker deployments, use `RedisEventStore` instead —
> SQLite is single-writer and not designed for shared multi-process access.
> Multiple processes writing the same database file contend on SQLite's file
> lock and will surface `SQLITE_BUSY` / "database is locked" errors.

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
    compression=None,       # "gzip" to compress large payloads (see "Large payloads")
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
{prefix}counter                 — atomic INCR source for monotonic event IDs (never expires)
{prefix}event:{event_id}        — HASH: stream_id + serialized payload
{prefix}stream:{stream_id}      — ZSET: event IDs sorted by score for O(log N) range queries
```

- **Atomic monotonic IDs** via Redis `INCR` — collision-free across concurrent workers. The counter is never given a TTL (even when `ttl` is set), so IDs stay monotonic across idle periods; only the event and stream keys expire.
- **Replay is O(log N + M)** — one `ZRANGEBYSCORE` range-scans the stream's sorted set, then each of the M matched events is fetched with its own `HGET`. That's one network round-trip per replayed event: fine for typical resume sizes, worth knowing for very long streams.
- **TTL support** — automatic expiry of event/stream keys to prevent unbounded memory growth
- **Atomic writes** — each event's hash, sorted-set entry, and TTLs are written in a single transactional pipeline, so a mid-write crash can't orphan a hash or leave a key without its expiry
- **Multi-tenant isolation** via configurable `key_prefix`
- **Priming event handling** — sentinel empty-string payloads are stored but never replayed to clients

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
needed), yield a ready store, and close the connection on exit — including when
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
  connection for their lifetime — size your pool for the expected number of
  concurrent subscribers. See [docs/production.md](docs/production.md) for sizing.

## Cross-backend migration: `migrate()`

Copy events from one store to another — e.g. SQLite → Postgres as a single-node
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

> **Caveats — read before migrating production data:** event IDs are *not*
> preserved (the destination assigns fresh ones), timestamps reset (TTL clock
> restarts), and resumability tokens are therefore invalidated — clients holding
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
synchronous methods — `on_store_event(stream_id, event_id, duration_ms)`,
`on_replay(stream_id, events_replayed, duration_ms)`, and
`on_error(operation, error)`. It does not need to subclass `MetricsCollector`
(it's a `Protocol`). A collector that raises is logged and ignored rather than
allowed to fail the underlying operation.

## Large payloads: `compression`

When MCP messages carry large tool results or big JSON-RPC bodies, pass
`compression="gzip"` to gzip-compress payloads above `compress_min_bytes`
(default `1024`) before they are stored — cutting storage and, on Redis, memory:

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
- **Redis Version:** 7.2.4 (Docker container on localhost)
- **PostgreSQL Version:** 16.2 (Docker container on localhost)

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

- **SQLite has the lowest latency _and_ the highest throughput** — it runs
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
- **Replay**: SQLite and Postgres fetch a stream's events in one indexed query, while the Redis backend issues a `zrangebyscore` followed by a single pipelined execution to fetch payloads concurrently — keeping the entire replay latency bounded to exactly two network round-trips.

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
