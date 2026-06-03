# Deploying mcp-persist in production

This guide covers what it takes to run an `mcp-persist` backend in production —
the gap between "it runs" and "it survives." It assumes you have already
[chosen a backend](../README.md#choosing-a-backend) and seen the
[quickstart](../README.md#quickstart). For runnable references, see
[`examples/`](../examples/).

Topics: [scope](#scope-what-resumability-does-and-does-not-cover) ·
[wiring](#1-wiring-it-into-your-app) ·
[reclaiming space](#2-reclaiming-space--schedule-purge_expired) ·
[schema & permissions](#3-schema--database-permissions) ·
[availability & failure modes](#4-high-availability--failure-modes) ·
[security](#5-security) · [scaling](#6-scaling-workers--nodes) ·
[observability](#7-observability) · [migrating](#8-migrating-between-backends) ·
[streaming](#9-real-time-streaming-with-subscribe) ·
[large payloads](#10-large-payloads-compression) ·
[readiness](#11-readiness-probes-ping) · [checklist](#production-checklist).

## Scope: what resumability does (and does not) cover

An `EventStore` persists exactly one thing: the **MCP transport events** (the
outbound JSON-RPC messages on an SSE stream) so a client that reconnects with a
`Last-Event-ID` can be replayed the messages it missed. That is all it does, and
it is easy to over-read.

It does **not** persist:

- **Tool / application state.** If a tool mutated in-memory state or had work in
  flight when the process died, that state is gone. Resumability replays the
  *messages* the server already produced; it does not re-run the tool or recover
  its progress.
- **Long-running tool calls.** If a tool was still executing at the cutover, the
  client must re-drive it. A replayed stream only contains messages that were
  emitted (and stored) before the interruption.
- **Session authentication / authorization.** Auth lives in your transport/app
  layer; the store holds serialized request/response payloads, not credentials or
  session identity.

The practical rule: resumability makes a **dropped connection** recoverable, not
a **lost computation**. Design long-running tools to be re-drivable by the client,
and treat the store as delivery durability for messages already sent — not as a
checkpoint of server-side work.

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

**Picking the backend from config.** To choose the backend at deploy time
without branching in code, build the store from the environment:

```python
from mcp_persist import event_store_from_env

# MCP_PERSIST_BACKEND=postgres
# MCP_PERSIST_URL=postgresql://localhost/mydb
# MCP_PERSIST_TTL=3600
async with event_store_from_env() as store:   # opens + closes the connection
    manager = StreamableHTTPSessionManager(app=mcp._mcp_server, event_store=store)
    async with manager.run():
        yield
```

It reads `MCP_PERSIST_BACKEND` (`sqlite` / `redis` / `postgres`) and
`MCP_PERSIST_URL`, plus optional `MCP_PERSIST_TTL`, `MCP_PERSIST_TABLE_NAME`
(SQLite/Postgres), and `MCP_PERSIST_KEY_PREFIX` / `MCP_PERSIST_MAX_STREAM_LENGTH`
(Redis), and returns that backend's `create()` context manager — so the
connection is opened on entry and closed on exit, exactly like the lifespan
above.

## 2. Reclaiming space — schedule `purge_expired()`

| Backend | Expiry | Your job |
|---|---|---|
| Redis | Native key TTL — Redis deletes expired keys automatically | Nothing |
| SQLite | None — `ttl` only **hides** expired events on replay | Call `purge_expired()` on a schedule |
| Postgres | None — same as SQLite | Call `purge_expired()` on a schedule |

For SQLite and Postgres, **if you never call `purge_expired()`, the table grows
without bound** — expired rows are skipped on replay but never deleted. The
shipped `PurgeScheduler` runs it on an interval for you; start it inside your
lifespan as an async context manager and it stops on exit:

```python
from mcp_persist import PurgeScheduler

        async with PurgeScheduler(store, interval=300):  # every 5 minutes
            async with manager.run():
                yield
```

It logs `purged N events` at `INFO`, swallows transient purge errors so the loop
survives a backend blip, and refuses a `RedisEventStore` (Redis expires keys
natively, so a scheduler would do nothing). Manage it explicitly with
`await scheduler.start()` / `await scheduler.aclose()` if you don't want a `with`
block.

**Batched purge under live traffic.** A single large `DELETE` can hold a lock
that contends with inserts and replay range-scans. Pass `batch_size=` to delete
expired rows in bounded chunks instead (committing per chunk), either directly or
via the scheduler:

```python
await store.purge_expired(batch_size=1000)          # one call, chunked
PurgeScheduler(store, interval=300, batch_size=1000) # scheduled, chunked
```

The expiry cutoff is captured once per call, so rows that expire mid-purge are
left for the next run. `batch_size=None` (the default) keeps the single-statement
`DELETE`, which is fine for low-to-moderate churn.

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

### Deployment topologies (rolling deploys, load balancers, serverless)

The store has to be **shared by every process that can serve a given client**.
A few common shapes get this wrong:

- **More than one replica / worker → SQLite will not do.** A local SQLite file is
  visible only to the process that opened it. Behind a load balancer (or during a
  **rolling deploy**, where a reconnecting client can land on a *different* pod
  than the one that issued its `Last-Event-ID`), that pod's database has none of
  the client's events and the resume silently returns nothing. Any replica count
  greater than 1 needs Redis or Postgres as a **shared** store. SQLite is for a
  genuine single process (single-node, edge, local dev).
- **Sticky sessions are a fragile substitute.** Pinning a client to one pod can
  paper over the above until that pod is replaced (deploy, scale-down, crash) —
  exactly when resumability is supposed to help. Prefer a shared store over
  relying on stickiness.
- **Serverless / read-only / ephemeral filesystems → Redis or Postgres only.**
  On a read-only or ephemeral disk (many serverless and container platforms),
  SQLite either fails to open the file or "succeeds" against scratch space that
  vanishes on the next invocation — durability you don't actually have. Don't
  mount SQLite on `/tmp` and assume it survives. Use a managed Redis/Postgres.

### Redis monotonic-counter throughput ceiling

Every `store_event` does one `INCR` on a single `{prefix}counter` key to mint the
next event ID. That is what keeps IDs globally monotonic across workers, but it
also means **all writes serialize through one key**. On a single Redis it is very
fast and rarely the bottleneck; on **Redis Cluster** that key lives on one node
and one shard, so it sets the ceiling on aggregate write throughput regardless of
cluster size. For the vast majority of MCP servers this is a non-issue — but if
you are pushing very high concurrent write rates, benchmark at *your* concurrency
(`benchmarks/benchmark.py --concurrency 500`) to find where it bends, and treat
the counter shard as the scaling limit rather than expecting it to fan out.

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

### Replay and pool connections (Postgres)

`replay_events_after` does **not** hold a pool connection for the whole replay.
It fetches the backlog in `replay_batch_size` chunks (default 500), and each
chunk is a discrete `pool.fetch()` that acquires and releases a connection; the
per-event `send_callback` (which can block on a slow SSE client) runs *between*
fetches, while no pooled connection is held. So a slow resuming client cannot pin
a connection and starve `store_event` writers. Size `max_size` for your peak
**concurrent resumes + normal write traffic**; lower `replay_batch_size` if large
payloads make a single chunk too big to hold in memory.

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
3. **Configure `max_stream_length`:** Set `max_stream_length` to cap the maximum number of event IDs stored in each stream's ZSET, preventing individual busy streams from growing too large. For high-churn deployments — lots of short-lived, one-off client streams — a modest cap such as `max_stream_length=1000` bounds each stream's index while still covering any realistic resume backlog (a client more than that many events behind only replays the most recent `max_stream_length`). Pair it with a `ttl` so the trimmed events' payload hashes expire rather than lingering:

   ```python
   RedisEventStore(redis_client, ttl=3600, max_stream_length=1000)
   ```

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
- **Readiness / liveness probes:** Each store exposes `await store.ping()`
  (Redis `PING`, Postgres/SQLite `SELECT 1`). It returns `True` when the backend
  is reachable and lets the driver error propagate otherwise, so a Kubernetes
  readiness probe or load-balancer health check can treat a raised exception as
  "not ready" and stop routing to a pod whose store is down.
- **Replay gap warnings:** `replay_events_after` logs a `WARNING` containing
  `Replay gap on stream` when a client's `Last-Event-ID` is still valid but
  events after it have expired/are missing and cannot be replayed — an
  unrecoverable gap the client would otherwise never learn about. A steady stream
  of these means your `ttl` is too short for how long clients stay disconnected;
  raise `ttl` (toward ≥ 2× `session_idle_timeout`) or investigate why clients
  resume so far behind.
- **What to monitor & alert on:**
  - **SDK Request Handler Failures:** Monitor and alert on logger outputs containing `Error in message router` or `Error in replay sender`. These are raised by the MCP SDK when the persistence store operations fail (e.g. connection timeout, locked DB).
  - **Purge loop results:** Monitor the return count of `store.purge_expired()` (or the `PurgeScheduler`'s `purged N events` log line). A count consistently at 0 while your database sizes grow indicates the loop has stalled or is not running.
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

## 10. Large payloads (`compression`)

If your MCP messages carry large tool results or big JSON-RPC bodies, the
serialized payload dominates storage and — on Redis — memory. Pass
`compression="gzip"` to gzip-compress payloads above a size threshold before they
are written:

```python
store = PostgresEventStore(pool, ttl=3600, compression="gzip", compress_min_bytes=1024)
```

- **Threshold.** Only payloads at least `compress_min_bytes` (default `1024`) are
  compressed; smaller messages are stored plain, since base64 framing would
  outweigh the saving. Compression is also discarded when it does not actually
  shrink a payload, so an incompressible body is never stored *larger*.
- **Transparent + backward compatible.** The stored form is marker-prefixed, and
  decompression on read is automatic and **independent of the setting** — a store
  with compression off still reads compressed payloads written by another store.
  So you can enable it on a rolling deploy (old and new payloads coexist) and
  `migrate()` across stores with mismatched settings. Existing data is untouched;
  turning the option on only affects newly written events.
- **Cost.** Compression spends CPU on the write path (and decompression on
  replay/subscribe). For mostly-small messages the threshold means you pay
  almost nothing; for large payloads the storage/memory win is usually worth it.
  Benchmark with your real message sizes if in doubt.

## 11. Readiness probes (`ping`)

Each store exposes `await store.ping()` for liveness/readiness checks — Redis
`PING`, Postgres/SQLite `SELECT 1`. It returns `True` when the backend is
reachable and lets the driver error propagate otherwise, so a health endpoint can
report "not ready" when the store's dependency is down:

```python
async def healthz(request):
    try:
        await request.app.state.store.ping()
    except Exception:
        return JSONResponse({"status": "unavailable"}, status_code=503)
    return JSONResponse({"status": "ok"})
```

Because the event store sits in the message-delivery path (see
[§4](#4-high-availability--failure-modes)), wiring `ping()` into your readiness
probe lets the orchestrator stop routing to a pod whose backend is unreachable
rather than letting it accept traffic it cannot serve.

## Production checklist

- [ ] `ttl` set to **≥ 2× `session_idle_timeout`** (never `None`)
- [ ] `purge_expired()` scheduled (SQLite/Postgres) — e.g. via `PurgeScheduler`,
      with `batch_size` under high churn; `VACUUM`/autovacuum considered
- [ ] Backend is HA / managed; connection + command timeouts set; the two SDK
      error logs alerted on; `ping()` wired into the readiness probe
- [ ] `Replay gap on stream` warnings alerted on (signals `ttl` too short)
- [ ] Schema pre-created **or** app role has DDL rights; correct
      `table_name` / `key_prefix` per tenant
- [ ] TLS enabled, credentials from a secret store, backend network-isolated
- [ ] Connection/pool size matched to worker concurrency; connection/pool closed
      on shutdown
- [ ] Backend matches your topology: SQLite = **single process only** (not behind
      a load balancer / rolling deploy, not on serverless/read-only FS);
      Redis/Postgres for any replica count > 1
- [ ] `compression="gzip"` considered if payloads are large
- [ ] If migrating backends with `migrate()`: run during a maintenance window
      with the source read-only; clients drained/reconnected afterwards (event
      IDs and resumability tokens change)
