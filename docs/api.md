# Programmatic features

Beyond drop-in SSE resumability, every store exposes a small set of building
blocks for streaming, migration, observability, and operations. For constructing
and configuring the stores themselves, see [backends.md](backends.md).

- [Real-time streaming: `subscribe()`](#real-time-streaming-subscribe)
- [Cross-backend migration: `migrate()`](#cross-backend-migration-migrate)
- [Metrics & observability](#metrics--observability)
- [Large payloads: `compression`](#large-payloads-compression)
- [Scheduled cleanup: `PurgeScheduler`](#scheduled-cleanup-purgescheduler)
- [Configuration from the environment: `event_store_from_env()`](#configuration-from-the-environment-event_store_from_env)
- [Readiness probes: `ping()`](#readiness-probes-ping)
- [Per-team retention policies](#per-team-retention-policies)
- [Event stream forking](#event-stream-forking)


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
  concurrent subscribers. See [production.md](production.md) for sizing.

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
> source first. See [production.md](production.md) for the full
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

### Proxy replay metric: `on_proxy_replay`

`PersistenceProxy` accepts the same collector via `metrics=` and recognizes one
**optional** extra method:

```python
def on_proxy_replay(self, stream_id, session_id, events_replayed, blocked, duration_ms):
    ...
```

It fires whenever a client reconnect triggers a replay, and is distinct from
`on_replay`: where `on_replay` counts what the store query returned,
`on_proxy_replay` reports what was actually delivered to the client *after* the
proxy's cross-session ownership gate, plus `blocked`, set `True` when a
`Last-Event-ID` resolved to another session's stream and was refused (so
`events_replayed` is `0`). Tracking the `blocked` rate surfaces clients
enumerating event IDs, and the `events_replayed` distribution shows how large
typical reconnect gaps are. The method is feature-detected, so an existing
three-method collector keeps working unchanged; `NoOpMetricsCollector` and
`LoggingMetricsCollector` both implement it.

```python
from mcp_persist import PersistenceProxy, LoggingMetricsCollector

async with PersistenceProxy.create(
    "http://localhost:8001", backend="sqlite", url="events.db", ttl=3600,
    metrics=LoggingMetricsCollector(),
) as proxy:
    ...
```

## Large payloads: `compression`

When MCP messages carry large tool results or big JSON-RPC bodies, pass a
`compression` codec to compress payloads above `compress_min_bytes` (default
`1024`) before they are stored, cutting storage and, on Redis, memory:

```python
store = PostgresEventStore(pool, ttl=3600, compression="zstd", compress_min_bytes=1024)
```

Two codecs are supported:

- `"gzip"`: always available (stdlib), no extra dependency.
- `"zstd"`: better ratio and speed for JSON-RPC payloads; needs the `zstd` extra
  (`pip install "mcp-persist[zstd]"`). Requesting it without the extra installed
  raises a clear `ValueError` at construction.

Each compressed payload is marker-prefixed (`gz:` or `zs:`), and decompression on
read is keyed entirely off that marker, so it is **independent of the store's own
setting**: a store with compression off (or set to the other codec) still reads
what another store wrote. That keeps a rolling deploy and `migrate()` across
mismatched settings safe, and lets you switch `gzip` to `zstd` without touching
existing data. Small or incompressible payloads are stored plain (never made
larger). Available on all three backends. Decompression is bounded by a 100 MiB
cap, so a crafted payload is rejected rather than materialized (a decompression
bomb guard).

## High-throughput writes: `BatchingEventStore`

When a deployment routes a lot of concurrent agentic traffic, the per-event round
trip to Redis or Postgres becomes the bottleneck before anything else does.
`BatchingEventStore` wraps a Redis or Postgres store and buffers writes, flushing
on whichever limit is hit first: a size threshold (`flush_max_events`, default
`64`) or a latency ceiling (`flush_max_latency_ms`, default `50`):

```python
from mcp_persist import BatchingEventStore, PostgresEventStore

inner = PostgresEventStore(pool, ttl=3600)
store = BatchingEventStore(inner, flush_max_events=64, flush_max_latency_ms=50)
# ... use as any EventStore ...
await store.aclose()  # flushes the tail
```

The wrapper still returns each event's `EventId` **synchronously** by
pre-allocating ID blocks from the inner store (Redis `INCRBY`, a Postgres sequence
batch), so resumability tokens are handed out immediately; only durability is
deferred to the flush window. The latency ceiling bounds that window, so the
worst case for a process crash before a flush is that a client replays from one
event earlier. `replay_events_after()` flushes pending writes first, so a
reconnecting client never misses a buffered event.

SQLite is intentionally rejected (a `TypeError` at construction): its own
write-behind (`commit_interval` / `commit_max_pending`) already batches the fsync
that dominates SQLite's write cost, so a second batching layer would add nothing.
Configure batching from the environment with `MCP_PERSIST_BATCH_MAX_EVENTS` and
`MCP_PERSIST_BATCH_MAX_LATENCY_MS` (on redis/postgres only).

## OpenTelemetry export: `OTelMetricsCollector`

`OTelMetricsCollector` implements the `MetricsCollector` interface on top of
OpenTelemetry instruments, so the persistence layer's timings and counts
correlate with the rest of a production stack's tracing instead of living in
isolation. It needs the `otel` extra (`pip install "mcp-persist[otel]"`):

```python
from opentelemetry import metrics
from mcp_persist import RedisEventStore
from mcp_persist.otel import OTelMetricsCollector

meter = metrics.get_meter("mcp-persist")
store = RedisEventStore(client, ttl=3600, metrics=OTelMetricsCollector(meter, backend="redis"))
```

It records `mcp_persist.store.duration_ms` and `mcp_persist.replay.duration_ms`
histograms plus `mcp_persist.errors` and `mcp_persist.proxy.replay` counters, all
tagged with `backend` (and `tenant_id` when you pass one). The hooks record
in-process and never block, matching the synchronous `MetricsCollector` contract.

## Tiered storage: `ArchiveScheduler` and `ChainedEventStore`

Instead of deleting expired events, you can archive them: move events past their
ttl out of a fast hot store (Redis, or a small SQLite/Postgres) into cold storage
(a larger Postgres, an S3-backed table, a `ttl=None` store) while keeping recent
events hot for fast resumption. See [tiered-storage.md](tiered-storage.md) for the
full design; in brief:

```python
from mcp_persist import ArchiveScheduler, ChainedEventStore

# Background archival: move expired batches hot -> cold on an interval.
async with ArchiveScheduler(hot, cold, interval=300, batch_size=500):
    ...

# Resume across both tiers: writes go to hot, replay falls back to cold on a miss.
store = ChainedEventStore(hot=hot, cold=cold)
```

`ArchiveScheduler` archives then deletes each batch (a crash can duplicate into
cold, which is upsert-safe, but never loses data before it is archived) and drains
the whole expired backlog each cycle. The lower-level `archive_expired_batch()`
and `count_expired()` helpers are exported for custom loops. Cold stores preserve
the original `event_id`, so resumability tokens stay valid across the tiers.

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
plus optional:

| Variable | Applies to | Effect |
| --- | --- | --- |
| `MCP_PERSIST_TTL` | all | event ttl in seconds |
| `MCP_PERSIST_TABLE_NAME` | sqlite, postgres | table name |
| `MCP_PERSIST_KEY_PREFIX` | redis | key prefix |
| `MCP_PERSIST_MAX_STREAM_LENGTH` | redis | per-stream cap |
| `MCP_PERSIST_TENANT_ID` | all | bind the store to one tenant (see [multi-tenancy.md](multi-tenancy.md)) |
| `MCP_PERSIST_COMPRESSION` | all | `gzip` or `zstd` payload codec |
| `MCP_PERSIST_BATCH_MAX_EVENTS` | redis, postgres | wrap in `BatchingEventStore` with this flush size |
| `MCP_PERSIST_BATCH_MAX_LATENCY_MS` | redis, postgres | batching flush latency ceiling |

Returns the chosen backend's `create()` context manager (wrapped in a
`BatchingEventStore` when a `MCP_PERSIST_BATCH_*` is set), so the connection is
opened on entry and closed on exit. Setting a `MCP_PERSIST_BATCH_*` with
`backend=sqlite` raises `ValueError` (SQLite uses write-behind instead).

## Readiness probes: `ping()`

Every store exposes `await store.ping()` (Redis `PING`, Postgres/SQLite
`SELECT 1`) for liveness/readiness checks. It returns `True` when the backend is
reachable and lets the driver error propagate otherwise, so a health endpoint can
report "not ready" when the store's dependency is down. See
[production.md](production.md#11-readiness-probes-ping).

## Per-team retention policies

When different teams require different event retention windows accompanied by a compliant, append-only deletion audit trail, use the retention policy components.

### `RetentionPolicy`

A frozen dataclass defining retention windows (in seconds) per tenant:

```python
from mcp_persist import RetentionPolicy

policy = RetentionPolicy(
    windows={
        "team-a": 86400,
        "team-b": 604800,
        None: 3600,
    },
    default=172800,
)
```

* `window_for(tenant_id)`: Return the retention window in seconds for the given tenant (or `None` to skip purging it).

### `retention_policy_from_env`

Build a `RetentionPolicy` from environment variables `MCP_PERSIST_RETENTION_WINDOWS` and `MCP_PERSIST_RETENTION_DEFAULT`. Returns `None` if unset.

### `RetentionScheduler`

A background scheduler that periodically checks and deletes expired events for each tenant:

```python
from mcp_persist import RetentionScheduler, DatabaseAuditSink

sink = DatabaseAuditSink(store)
async with RetentionScheduler(store, policy, sink, interval=300.0):
    # Runs in the background
    ...
```

* `strict_audit=True` (default): Propagates audit sink exceptions to force operator attention under logging failures.

### Audit Sinks

Pluggable destinations conforming to the `AuditSink` protocol (`record(entry)` method):

* `NoOpAuditSink`: Discards all entries.
* `LoggingAuditSink`: Writes entries as JSON lines to the python logger.
* `DatabaseAuditSink`: Appends entries to a dedicated table (`{events_table}_retention_audit`).


## Event stream forking

Event stream forking allows branching an existing session (`stream_id`) at any point (a specific `fork_event_id`), letting clients/runners replay from that branch with different inputs or a different model while preserving the original branch intact. This turns the linear event log history into a tree for systematic A/B evaluation.

```python
# Fork 'orig-stream' at event '123' into a new branch 'fork-stream'
await store.fork_stream("orig-stream", "123", "fork-stream")

# Replay events for the branched session starting from the beginning
await store.replay_events_after("0", send_callback, "fork-stream")
```

- **Ancestry resolution:** replaying or iterating over a child stream dynamically traverses all ancestor stream segments up to the root, only scanning the slice of events valid for that segment.
- **Segment scope:** replaying handles boundary constraints (`min_id` and `max_id`) per segment segmentally.
- **Unified interfaces:** supported natively by SQLite, Redis, and Postgres backends, as well as `BatchingEventStore` and `ChainedEventStore`.

