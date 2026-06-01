# Deploying mcp-persist in production

This guide covers what it takes to run an `mcp-persist` backend in production —
the gap between "it runs" and "it survives." It assumes you have already
[chosen a backend](../README.md#choosing-a-backend) and seen the
[quickstart](../README.md#quickstart). For runnable references, see
[`examples/`](../examples/).

Topics: [wiring](#1-wiring-it-into-your-app) ·
[reclaiming space](#2-reclaiming-space--schedule-purge_expired) ·
[schema & permissions](#3-schema--database-permissions) ·
[availability & failure modes](#4-high-availability--failure-modes) ·
[security](#5-security) · [scaling](#6-scaling-workers--nodes) ·
[observability](#7-observability) · [checklist](#production-checklist).

## 1. Wiring it into your app

Create the store **once** at startup and share it across requests. The store
wraps a connection/pool that **you own** — `mcp-persist` never opens or closes
it for you, so create it on startup and close it on shutdown. The canonical
pattern is an ASGI lifespan (Starlette/FastAPI shown; condensed from the
examples):

```python
import contextlib

import asyncpg
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp_persist import PostgresEventStore

mcp = FastMCP(name="MyServer")

@contextlib.asynccontextmanager
async def lifespan(app):
    pool = await asyncpg.create_pool(DSN, min_size=2, max_size=10)
    try:
        store = PostgresEventStore(pool, ttl=3600)
        await store.initialize()  # SQLite/Postgres only; Redis has no initialize()

        manager = StreamableHTTPSessionManager(
            app=mcp._mcp_server,            # the low-level Server FastMCP wraps
            event_store=store,
            session_idle_timeout=300,       # seconds
        )
        app.state.session_manager = manager
        async with manager.run():
            yield
    finally:
        await pool.close()                  # you opened it, so you close it
```

**Size `ttl` to your sessions.** Set `ttl` to **at least 2×
`session_idle_timeout`** so a client that idles right up to the timeout can
still resume. Leaving `ttl=None` logs a warning and lets events accumulate
forever — treat that as a misconfiguration in production.

## 2. Reclaiming space — schedule `purge_expired()`

| Backend | Expiry | Your job |
|---|---|---|
| Redis | Native key TTL — Redis deletes expired keys automatically | Nothing |
| SQLite | None — `ttl` only **hides** expired events on replay | Call `purge_expired()` on a schedule |
| Postgres | None — same as SQLite | Call `purge_expired()` on a schedule |

For SQLite and Postgres, **if you never call `purge_expired()`, the table grows
without bound** — expired rows are skipped on replay but never deleted. Run it
periodically from a background task:

```python
import asyncio
import logging

async def purge_loop(store, interval_seconds: int = 300):
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            removed = await store.purge_expired()  # returns rows deleted
            if removed:
                logging.info("purged %d expired events", removed)
        except Exception:
            logging.exception("purge_expired failed")  # keep the loop alive
```

Start it inside your lifespan and cancel it on shutdown:

```python
        task = asyncio.create_task(purge_loop(store, 300))
        try:
            async with manager.run():
                yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
```

Reclaiming **disk**, not just rows:

- **SQLite** — `DELETE` frees pages for reuse but does not shrink the file. If
  disk footprint matters, run `VACUUM` during a quiet window.
- **Postgres** — `DELETE` leaves dead tuples that autovacuum reclaims over time;
  for high churn, ensure autovacuum is tuned or schedule the purge with
  [`pg_cron`](https://github.com/citusdata/pg_cron) so cleanup runs inside the
  database with no external scheduler.

## 3. Schema & database permissions

On first use (`initialize()`, called automatically), the SQLite and Postgres
backends run `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS` (SQLite
also sets `PRAGMA journal_mode=WAL`). This is idempotent and safe to call at
every startup; concurrent first calls on a pool are serialized by an internal
lock (1.0.1+).

If your application's database role is **not allowed to run DDL** (common in
locked-down environments), pre-create the schema with an admin role and run the
app with a DML-only role. The exact schema (`mcp_events` is the default
`table_name`):

**Postgres**
```sql
CREATE TABLE IF NOT EXISTS mcp_events (
    event_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_id  TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS mcp_events_stream_idx ON mcp_events (stream_id, event_id);
```

**SQLite**
```sql
CREATE TABLE IF NOT EXISTS mcp_events (
    event_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id  TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS mcp_events_stream_idx ON mcp_events (stream_id, event_id);
```

## 4. High availability & failure modes

The event store sits in the **message-delivery path**: the MCP SDK calls
`store_event` for every outbound message before delivering it, and
`replay_events_after` on every resume. Verified behavior of the installed SDK
when a backend call raises (e.g. the database is unreachable):

- **`store_event` failure** is caught by the SDK's message router and logged as
  `Error in message router`. The server process keeps running and other sessions
  are unaffected, **but that session's outbound message routing stops** until the
  client reconnects — in-flight responses on that session may not be delivered.
- **`replay_events_after` failure** is caught and logged as `Error in replay
  sender`. The resume degrades (the client gets no replayed events) but the
  server keeps running.

The practical implication: **treat the backend as a critical dependency.** Use a
managed or replicated Redis/Postgres, set sane connection and command timeouts,
and alert on the two log lines above. Pointing the store at a best-effort or
frequently-restarted instance will surface as dropped messages and failed
resumes, not as a clean error.

## 5. Security

- **Keep credentials out of code.** The examples hard-code connection strings for
  brevity; in production read them from the environment or a secret manager.
- **Use TLS in transit.** Redis: `rediss://host:6380/0`. Postgres:
  append `?sslmode=require` (or `verify-full`) to the DSN.
- **Authenticate.** Redis `AUTH` via URL userinfo
  (`redis://:password@host`); a dedicated, least-privilege Postgres role.
- **Network-isolate** the backend (private subnet / security group) — it holds
  serialized request/response payloads.
- **Isolate tenants** with a distinct `key_prefix` (Redis) or `table_name`
  (SQLite/Postgres) per logical server — see
  [multi-tenant deployments](../README.md#multi-tenant-deployments).

## 6. Scaling: workers & nodes

| Backend | Topology | Notes |
|---|---|---|
| SQLite | **Single process only** | One writer. Multiple processes on the same file contend on the file lock and raise `SQLITE_BUSY` / "database is locked". Ideal for single-node / edge. |
| Redis | Many workers / replicas | All instances share one Redis; event IDs stay globally monotonic via atomic `INCR`, and the ID counter never expires (1.0.1+). [Size the client connection pool](#redis-connection-pool-sizing) to your concurrency and watch memory. |
| Postgres | Multi-node / team | Safe across nodes via `IDENTITY` + a pooled connection. Size the `asyncpg` pool (`max_size`) to match per-instance concurrency; rely on autovacuum plus the scheduled purge. |

### Redis connection pool sizing

`RedisEventStore` issues **two round-trips per `store_event`** (an atomic `INCR`
for the ID, then a pipeline for the event hash and stream index), and the SDK
calls `store_event` for every outbound message. Under high SSE fan-out — many
concurrent streams — those connections are drawn from the pool of the
`redis.asyncio` client **you** construct and pass in.

That pool matters because of a difference from `asyncpg`: `redis.asyncio.from_url(...)`
defaults to **`max_connections=100`** and, once the pool is exhausted, **raises**
a `MaxConnectionsError` (a `ConnectionError`, `"Too many connections"`) instead of
waiting. `asyncpg`, by contrast, *queues* callers until a connection frees up. So a
burst of concurrent writes that merely slows down on Postgres can fail outright on
Redis with the default pool.

Size the pool to your peak concurrency when you build the client:

```python
import redis.asyncio as aioredis

redis_client = aioredis.from_url(REDIS_URL, max_connections=512)
store = RedisEventStore(redis_client, ttl=3600)
```

Or use a `BlockingConnectionPool`, which waits for a free connection (like
`asyncpg`) rather than raising:

```python
from redis.asyncio import BlockingConnectionPool, Redis

pool = BlockingConnectionPool.from_url(REDIS_URL, max_connections=128, timeout=5)
redis_client = Redis(connection_pool=pool)
```

A `MaxConnectionsError` surfacing through the SDK's `Error in message router`
log (see [§4](#4-high-availability--failure-modes)) is the signal that the pool is
undersized for your load.

### Subscribers and connection pools (`subscribe()`)

If you use the real-time [`subscribe()`](#9-real-time-streaming-with-subscribe)
API, budget pool capacity for it on **both** backends — each active subscriber
holds a connection for the lifetime of the subscription:

- **Redis**: `subscribe()` uses `client.pubsub()`, which draws a dedicated
  connection from the same pool as `store_event`/`replay_events_after`. With *N*
  concurrent subscribers you need *N* connections beyond your write/replay
  traffic; size `max_connections` (or use a `BlockingConnectionPool`) accordingly,
  exactly as above.
- **Postgres**: asyncpg requires a dedicated connection for `LISTEN`, so each
  subscriber calls `pool.acquire()` and holds it until the subscription ends.
  Size `max_size` for **peak concurrent subscribers + normal store/replay
  concurrency**. If the pool is exhausted, `store_event`/`replay_events_after`
  (and new subscriptions) block waiting for a free connection. A deployment with
  many long-lived subscribers should run a pool large enough for all of them, or
  use a separate pool/store instance dedicated to subscriptions.

SQLite's `subscribe()` polls the table on the store's existing connection and
opens no new connections, so this does not apply to it.

### Redis memory & stream cardinality growth

When scaling a server with millions of unique client streams, Redis stores:
- A global `{prefix}counter` (never expires).
- One `{prefix}event:{event_id}` HASH key per event.
- One `{prefix}stream:{stream_id}` ZSET key per unique stream.

While individual event hashes and stream ZSETs expire automatically when `ttl` is set, a system with a very high rate of unique stream creation (e.g. one-off client connections) can accumulate millions of ZSET keys in memory within the TTL window.

**Strategies to manage memory and cardinality:**
1. **Always configure a TTL:** Set a reasonable `ttl` on `RedisEventStore` so inactive streams and their events are automatically evicted by Redis.
2. **Use `volatile-lru` or `volatile-ttl` eviction policy:** Configure Redis with an eviction policy that only targets keys with an expiration time set. **Do not use `allkeys-lru` or `allkeys-random`**, as these can evict the global `{prefix}counter` key (which has no TTL). If the counter key is evicted, the ID sequence resets, breaking stream resumability guarantees.
3. **Configure `max_stream_length`:** Set `max_stream_length` to cap the maximum number of event IDs stored in each stream's ZSET, preventing individual busy streams from growing too large.

## 7. Observability

- **Explicitly configure loggers:** The library logs warnings, errors, and informational updates using standard Python `logging`. Explicitly configure levels and handlers for the following loggers to capture key operational events:
  - `mcp_persist.redis`
  - `mcp_persist.sqlite`
  - `mcp_persist.postgres`

  You can configure individual backends or configure the parent `mcp_persist` namespace:
  ```python
  import logging

  # Set log levels for all backends at once via parent namespace
  logging.getLogger("mcp_persist").setLevel(logging.WARNING)

  # Or configure specific backends individually
  logging.getLogger("mcp_persist.redis").setLevel(logging.INFO)
  logging.getLogger("mcp_persist.sqlite").setLevel(logging.DEBUG)
  ```
- **Metrics hooks:** For timing and throughput data rather than logs, pass a `MetricsCollector` to any store via `metrics=`. Its `on_store_event(stream_id, event_id, duration_ms)`, `on_replay(stream_id, events_replayed, duration_ms)`, and `on_error(operation, error)` hooks let you feed per-operation latency and counts into Prometheus, Datadog, etc. The default (no collector) takes a zero-overhead fast path. A misbehaving collector that raises is logged and ignored — it can never fail a store or replay. `LoggingMetricsCollector` ships built in for a quick `DEBUG`-level view; see [`metrics.py`](../src/mcp_persist/metrics.py).
- **Construction warning alerts:** A `WARNING` log emitted at construction (e.g. `SQLiteEventStore created with ttl=None`) means events will accumulate indefinitely. Set up alert rules to detect this warning in production, as it signals a deploy-time misconfiguration.
- **Tolerated Catalog Race events:** At `DEBUG` level, the engines log tolerated catalog creation races (e.g. `Tolerating concurrent DDL race on...`) which are helpful to ignore/diagnose during scale-outs.
- **What to monitor & alert on:**
  - **SDK Request Handler Failures:** Monitor and alert on logger outputs containing `Error in message router` or `Error in replay sender`. These are raised by the MCP SDK when the persistence store operations fail (e.g. connection timeout, locked DB).
  - **Purge loop results:** Monitor the return count of `store.purge_expired()`. A count consistently at 0 while your database sizes grow indicates the loop has stalled or is not running.
  - **Database health metrics:** Backend CPU usage, query latency, active connection pool counts, and Redis memory statistics (e.g., `used_memory` and key evictions).

## 8. Migrating between backends

`migrate(source, dest)` copies events from one store to another — e.g. SQLite →
Postgres as a single node grows into a cluster, or Redis → Postgres for
durability. It streams events oldest-first and re-stores them on the
destination, preserving per-stream ordering. `list_streams()` (available on every
backend) enumerates the streams; pass `stream_id=` to scope to a single one.

```python
from mcp_persist import migrate

result = await migrate(
    sqlite_store,
    postgres_store,
    on_progress=lambda sid, n: log.info("migrating %s: %d events", sid, n),
)
log.info(
    "migrated %d events across %d streams; failed: %s",
    result.events_migrated,
    result.streams_migrated,
    result.failed_streams,
)
```

Each stream is migrated independently: a stream that errors is logged, recorded
in `result.failed_streams`, and skipped so the rest of the run still completes
(a failed stream may have been partially copied).

**Read these caveats before migrating a production deployment:**

- **Event IDs are not preserved.** The destination issues its own fresh,
  monotonic IDs via `store_event`. Ordering and payloads are preserved; the
  numeric IDs are not.
- **Timestamps are reset.** Re-stored events get a `created_at` of "now", so any
  `ttl` expiry clock restarts on the destination. Run `purge_expired()` on the
  source first if you don't want already-stale events carried over.
- **Resumability tokens are invalidated.** Because IDs change, a client holding a
  `Last-Event-ID` issued by the source store cannot resume against the
  destination after cutover. **Migrate during a maintenance window and
  drain/reconnect clients afterwards.**
- **Not consistent under concurrent writes.** `migrate` is a point-in-time copy;
  events written to the source while it runs may or may not be picked up. Treat
  the source as read-only (stop writes) for a complete, consistent copy.

## 9. Real-time streaming with `subscribe()`

`subscribe(stream_id)` is an async generator that yields `(event_id, message)`
as events are written, instead of polling `replay_events_after`:

```python
store = RedisEventStore(client, ttl=3600, enable_streaming=True)

async for event_id, message in store.subscribe("stream-abc"):
    handle(message)
```

It must be opted into with `enable_streaming=True`. On Redis and Postgres that
flag also makes `store_event` publish a lightweight notification after each
non-priming write (`PUBLISH` / `pg_notify`); with the default `False` there is
no extra round-trip and `subscribe()` raises. SQLite has no native push, so its
`subscribe()` polls the table every `poll_interval` seconds (default `0.5`) and
the flag only gates the method.

**It is a best-effort, forward-only feed — not a durability mechanism:**

- Only events written **after** the subscription registers are delivered. Use
  `replay_events_after` to catch up on history.
- Redis pub/sub and Postgres `NOTIFY` are **at-most-once**: anything emitted
  while no subscriber is connected, or during a reconnect, is dropped. The
  notification publish is best-effort and a failure is logged without failing
  the write. **`replay_events_after` remains the durable, gap-free path** — a
  robust consumer pairs `subscribe()` for low latency with a periodic replay (or
  a replay on reconnect) for completeness.
- **A dropped connection may not be surfaced as an error.** This is most acute
  on **Postgres**: the subscriber waits on a local notification queue, so if the
  connection dies — e.g. the server restarts — no further notifications arrive
  and the `async for` simply goes quiet rather than raising. (Redis pub/sub
  reads from the socket and usually *raises* on a broken link, ending the
  generator, but a half-open connection can still stall.) Do not treat silence
  as liveness: keep an application-level heartbeat / ping on the session to
  detect a stalled subscription and reconnect, and lean on `replay_events_after`
  after any reconnect to close the gap.
- Priming events and payloads that fail JSONRPC validation are skipped.

SQLite's `subscribe()` polls the store's single connection, so it competes with
writes for SQLite's one writer: a low `poll_interval` and/or many concurrent
subscribers will measurably cut write throughput. Keep subscriber counts low and
`poll_interval` at or above the default on SQLite, or use Redis/Postgres for
high-volume streaming.

For the connection-pool impact of running many subscribers, see
[Subscribers and connection pools](#subscribers-and-connection-pools-subscribe).

## Production checklist

- [ ] `ttl` set to **≥ 2× `session_idle_timeout`** (never `None`)
- [ ] `purge_expired()` scheduled (SQLite/Postgres); `VACUUM`/autovacuum considered
- [ ] Backend is HA / managed; connection + command timeouts set; the two SDK
      error logs alerted on
- [ ] Schema pre-created **or** app role has DDL rights; correct
      `table_name` / `key_prefix` per tenant
- [ ] TLS enabled, credentials from a secret store, backend network-isolated
- [ ] Connection/pool size matched to worker concurrency; connection/pool closed
      on shutdown
- [ ] Backend matches your topology (SQLite = single process; Redis/Postgres =
      multi-worker)
- [ ] If migrating backends with `migrate()`: run during a maintenance window
      with the source read-only; clients drained/reconnected afterwards (event
      IDs and resumability tokens change)
