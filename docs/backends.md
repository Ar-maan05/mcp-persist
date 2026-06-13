# Backends

Reference for constructing and configuring each `EventStore` backend by hand, plus
the connection-lifecycle helper. For the high-level "which backend should I use?"
decision and the `with_persistence()` one-liner, see the
[README](../README.md#backends--choosing-one).

- [Manual wiring (advanced or non-FastMCP)](#manual-wiring-advanced-or-non-fastmcp)
- [SQLiteEventStore](#sqliteeventstore)
- [RedisEventStore](#rediseventstore)
- [PostgresEventStore](#postgreseventstore)
- [Connection lifecycle: `create()`](#connection-lifecycle-create)

## Manual wiring (advanced or non-FastMCP)

`with_persistence()` is the fast path on FastMCP. When you're not on FastMCP, or
you want to own the wiring yourself, construct a store and hand it to
`StreamableHTTPSessionManager`. The backends are interchangeable; pick per
[Choosing a backend](../README.md#backends--choosing-one).

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
    compression=None,       # "gzip" to compress large payloads (see "Large payloads" in api.md)
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
[production.md](production.md#12-write-behind-commits-sqlite) for the
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
    compression=None,       # "gzip" to compress large payloads (see "Large payloads" in api.md)
)
```

- **TTL guidance:** Set `ttl` to at least 2× your session idle timeout. If you leave it as `None`, a warning is logged and events accumulate indefinitely.
- **Stream bounds (`max_stream_length`):** Set a positive integer to cap the size of each stream's sorted set. The oldest event IDs beyond this limit are automatically trimmed on every write, preventing unbounded memory growth on long-lived streams.

### Production Note: Stream Cardinality & Redis Memory Growth

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
    compression=None,         # "gzip" to compress large payloads (see "Large payloads" in api.md)
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
[Manual wiring](#manual-wiring-advanced-or-non-fastmcp).
