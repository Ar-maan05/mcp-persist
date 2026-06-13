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
[production.md](production.md#11-readiness-probes-ping).
