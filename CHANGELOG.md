# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **`examples/resume_demo.py`** — a self-contained, recordable terminal demo of resumability in a single command (`python examples/resume_demo.py`). It runs a real FastMCP + `SQLiteEventStore` server in a background thread, starts a streaming tool call, yanks the connection mid-stream to simulate a client/network crash, then reconnects with `Last-Event-ID` and watches the server replay exactly the missed events before delivering the tool's result. Nothing is mocked — events round-trip through SQLite (`resume_demo.db`) and the client speaks the Streamable HTTP wire protocol, parsing the SSE stream with mcp-persist's own `SSEParser`. Needs only the `[sqlite]` extra (uvicorn + httpx already ship with `mcp`).

### Changed
- README now reflects the current test suite size (300+ async tests across all three backends), replacing the stale earlier count.

## [1.7.0] - 2026-06-05

### Added
- **Persistence proxy** (`mcp_persist.PersistenceProxy` + the `mcp-persist-proxy` CLI):
  - An ASGI app that adds SSE stream resumability in front of **any** upstream MCP server without modifying it. It forwards requests upstream and, for `text/event-stream` responses, intercepts the stream: each event is parsed, persisted to an `EventStore` (which assigns the proxy's own monotonic event ID), and forwarded to the client. A client that drops can reconnect with `Last-Event-ID`; the proxy replays the missed events from the store and then continues live. The upstream runs **without** its own event store — the proxy is the store. The buffer outlives the client request, so a disconnect mid-response keeps storing and a later reconnect gets a complete history.
  - Scope is honest: it survives **client** disconnects against a stable upstream. It does not survive an upstream restart (new session, new IDs), and an event that neither the client nor the proxy received before storage is gone — same at-most-once guarantee as the SDK itself.
  - Store resolution mirrors `with_persistence`: `PersistenceProxy.create(upstream, store=...)` (caller-owned), `backend=`+`url=`(+`ttl=`) built and closed for you, or neither → `event_store_from_env()` (`MCP_PERSIST_*`). `create()` owns the shared `httpx.AsyncClient` and the store/buffer lifecycle.
  - Follows upstream redirects internally (e.g. a server's `/mcp` → `/mcp/` trailing-slash redirect), so the client sees a clean endpoint and never gets bounced past the proxy to the upstream.
  - **CLI** (`mcp-persist-proxy`): point at a running upstream (`--upstream URL --backend sqlite --url events.db [--port 8000] [--path /mcp]`), or start one as a subprocess, wait for it to come up, and proxy it (`--backend redis --url ... [--upstream-port 8001] -- uvicorn my_server:app --port 8001`) — the child is stopped (SIGTERM, then SIGKILL) when the proxy exits.
  - **No new dependencies** — `httpx` and `uvicorn` already ship transitively with `mcp`, so the proxy needs no extra install. New modules `mcp_persist/proxy.py`, `mcp_persist/_stream_buffer.py`, `mcp_persist/_sse_parser.py`, `mcp_persist/_cli.py`, with unit tests for the SSE parser, the stream buffer (cold/hot replay, live fan-out, disconnect survival), the proxy (JSON passthrough, POST/GET SSE, reconnect to a live buffer vs. store-only replay, mid-stream disconnect), and CLI argument handling.
  - Documented in the README ("Resumability without touching the server") and the [production guide](docs/production.md) (proxy mode: single point of failure, the shared-store requirement across proxy replicas, and the upstream-restart scope boundary).

## [1.6.0] - 2026-06-04

### Added
- **FastMCP plugin** (`mcp_persist.with_persistence`):
  - Takes a `FastMCP` instance and returns a runnable Starlette ASGI app with SSE stream resumability already wired in — collapsing the manual dance of opening a connection, building an `EventStore`, constructing a `StreamableHTTPSessionManager`, and writing a Starlette lifespan down to a single call. The returned app owns the store + session-manager lifecycle (opened on startup, closed on shutdown) and mounts the MCP endpoint at `mcp_path` (default `/mcp`).
  - Three ways to supply the store, resolved in order: a pre-built `store=` (caller owns its lifecycle — **not** closed on shutdown); `backend=` + `url=` config kwargs (`ttl`/`table_name` for sqlite & postgres, `key_prefix`/`max_stream_length` for redis), built and closed for you; or neither, falling back to `event_store_from_env()` (`MCP_PERSIST_*`). Conflicting or inapplicable arguments (e.g. `store=` together with `backend=`, a redis-only option on sqlite, or config kwargs with no backend) raise `ValueError` rather than being silently ignored. The live store is exposed on `app.state.event_store` so a `PurgeScheduler` can run alongside the server.
  - **No new dependencies** — `starlette` and `StreamableHTTPSessionManager` already ship with `mcp`. New example `examples/fastmcp_plugin_server.py` and end-to-end tests in `tests/test_fastmcp_plugin.py` (real MCP client over an in-process server, asserting events persist and replay through the plugin-wired store).

## [1.5.0] - 2026-06-04

### Added
- **SQLite write-behind commits** (`commit_interval`, `commit_max_pending`):
  - `SQLiteEventStore` (and `SQLiteEventStore.create()`) accept an optional `commit_interval` (seconds). When set, `store_event` no longer commits on every call; instead the insert stays in SQLite's open transaction and a background task commits all buffered events every `commit_interval` seconds — one `fsync` per interval instead of one per event, trading a bounded durability window for substantially higher write throughput. Buffered events remain **immediately visible to `replay_events_after` and `subscribe` on the same store** (read-your-writes within the process); only a hard crash loses the uncommitted tail (≤ one interval). `commit_max_pending` additionally commits inline once that many events are buffered, bounding the loss window by count and capping the open transaction size under bursts; it can be used alone for pure count-based group commit. Both default to `None`, so the existing durable commit-per-event behavior is unchanged.
  - New lifecycle: `SQLiteEventStore` is now an async context manager and exposes `await store.aclose()`, which stops the flusher and commits the final batch. **Write-behind requires closing the store** (via `create()`, `async with store:`, or `aclose()`) or the last interval of events is dropped on shutdown; `create()`'s context manager flushes and closes for you. `aclose()` is idempotent and a no-op when write-behind is off.
  - Docs: new "Write-behind commits (SQLite)" sections in the README and `docs/production.md` (durability trade-off, the mandatory-close footgun, the single-writer caveat, and a production-checklist item).

### Fixed
- **Corrupt or partial compressed payloads no longer crash replay, `subscribe`, or migration.** A `gz:`-marked payload that was truncated or otherwise not valid gzip/base64 raised `gzip.BadGzipFile` / `binascii.Error` from `decompress_payload`, which escaped the `ValidationError`-only guard and aborted the entire stream (or migration run) instead of skipping the one bad event. All three backends now skip an undecodable event — logging a warning with the underlying error — and continue, exactly as they already tolerated a single payload that failed JSON-RPC validation. Regression coverage in `tests/test_bug_finding_corruption.py`, plus new stress (`tests/test_stress_*.py`) and integrity (`tests/test_integrity_all.py`) suites.

## [1.4.0] - 2026-06-03

### Added
- **Payload compression** (`compression`, `compress_min_bytes`):
  - All three stores accept `compression="gzip"` to gzip-compress event payloads above `compress_min_bytes` (default `1024`) before storing them, cutting storage and (on Redis) memory for large tool results / JSON-RPC bodies. The stored form is marker-prefixed (`gz:` + base64), so it can never collide with a real payload, and **decompression on read is automatic and independent of the setting** — a store reads compressed payloads written by another store even with compression disabled, so the option is safe to roll out incrementally and across `migrate()`. Compression is only kept when it actually shrinks the payload, so small/incompressible messages are never made larger. Default is `None` (no compression); existing data is unaffected.
- **Batched purge** (`purge_expired(batch_size=...)`):
  - `SQLiteEventStore.purge_expired` and `PostgresEventStore.purge_expired` now accept an optional `batch_size`. When set, expired rows are deleted in bounded chunks (SQLite via an indexed `event_id` subselect; Postgres via a `ctid` subselect) committing per chunk, so a large purge does not hold one long lock that contends with live inserts and replay scans. The expiry cutoff is captured once up front. `batch_size=None` (the default) keeps the previous single-statement `DELETE`.
- **Replay gap detection**:
  - `replay_events_after` now logs a `WARNING` when the anchor (`Last-Event-ID`) still exists but one or more events after it have expired / are missing and will be silently skipped — surfacing an otherwise invisible, unrecoverable gap to a reconnecting client. SQLite/Postgres detect this with a `ttl`-gated existence check; Redis warns when stream-index entries after the anchor have lost their payloads. `replay_events_after` still returns normally and delivers every event it can.
- **`ping()` readiness probe**:
  - All three stores expose `async ping() -> bool` (Redis `PING`, Postgres/SQLite `SELECT 1`) for liveness/readiness probes. Returns `True` on success and lets connection errors propagate so a probe can treat a raised exception as "not ready".
- **`PurgeScheduler`**:
  - A batteries-included async context manager / `start()`+`aclose()` wrapper that runs `purge_expired()` on an interval (with optional `batch_size`), logs `purged N events`, and swallows transient errors so the loop survives a backend blip. Rejects stores without `purge_expired` (e.g. `RedisEventStore`, which expires keys natively) at construction. Replaces the hand-rolled purge-loop snippet from `docs/production.md`. Exported from `mcp_persist`.
- **`event_store_from_env()`**:
  - Builds a store from `MCP_PERSIST_BACKEND` / `MCP_PERSIST_URL` (+ optional `MCP_PERSIST_TTL`, `MCP_PERSIST_TABLE_NAME`, `MCP_PERSIST_KEY_PREFIX`, `MCP_PERSIST_MAX_STREAM_LENGTH`) and returns the matching backend's `create()` context manager, so a deployment can pick its store from config without branching on the backend. Exported from `mcp_persist`.
- **Tooling & docs**:
  - `compose.yaml` at the repo root spins up local Redis + Postgres for the examples and for running the test suite against real backends.
  - New `docs/production.md` material: deployment topologies (rolling deploys / load balancers without sticky sessions, serverless / read-only filesystems), the scope boundary of resumability ("what it does *not* give you"), the Redis monotonic-counter throughput ceiling, and guidance for the new features above.
  - **Multi-process integration test**: a separate OS process writes events to a shared Redis/Postgres while the test process concurrently replays them, validating cross-process resumability (the reason to choose Redis/Postgres over single-writer SQLite). Gated on backend availability.

## [1.3.0] - 2026-06-01

### Changed
- **RedisEventStore**: now logs a warning at construction when `max_stream_length` is set together with `ttl=None`. Trimming the stream index to `max_stream_length` drops old event IDs but does not delete their payload hashes, so without a `ttl` those payloads never expire and accumulate in Redis indefinitely. Pair `max_stream_length` with a `ttl` so trimmed payloads expire on their own.

### Added
- **Push-based streaming**:
  - `subscribe(stream_id)` async generator on all three backends — yields `(event_id, message)` for events as they are written, instead of polling `replay_events_after`. Backed by Redis pub/sub, Postgres `LISTEN`/`NOTIFY`, and an SQLite polling fallback (`poll_interval`, default 0.5s). Opt in with the new `enable_streaming=True` constructor flag; with the default `False` there is no extra per-write round-trip and `subscribe()` raises, so existing behavior is unchanged. Delivery is **best-effort and forward-only (at-most-once)**: only events written after the subscription registers are delivered, the notification publish is best-effort (a failure is logged, never failing the write), and `replay_events_after` remains the durable catch-up path. Subscriptions are cancellable and release their connection on teardown. See the new "Real-time streaming with `subscribe()`" and "Subscribers and connection pools" sections in `docs/production.md` (each Postgres subscriber holds a pool connection for its lifetime — size the pool accordingly).
- **Cross-backend migration**:
  - `migrate(source, dest)` — copies every event from one store to another, preserving per-stream ordering and payloads. Supports `stream_id=` scoping to a single stream, an `on_progress` callback, and a configurable `batch_size`. Streams are migrated independently and returned in a `MigrationResult` (`streams_migrated`, `events_migrated`, `failed_streams`); a stream that errors is logged and skipped rather than aborting the run. Priming (empty-payload) events are copied faithfully. Note: event IDs and `created_at` timestamps are **not** preserved (the destination issues fresh IDs and timestamps), so client `Last-Event-ID` resumability tokens are invalidated by a migration — see the new "Migrating between backends" section in `docs/production.md`. `migrate` and `MigrationResult` are exported from `mcp_persist`.
  - `list_streams()` on all three backends — yields each distinct stored stream ID; backs whole-database migration.
- **Metrics / observability**:
  - `MetricsCollector` protocol with optional `metrics=` hooks on all three stores. Implement `on_store_event(stream_id, event_id, duration_ms)`, `on_replay(stream_id, events_replayed, duration_ms)`, and `on_error(operation, error)` to emit timing and count data to Prometheus, Datadog, logs, or anything else. When no collector is supplied the store uses a `NoOpMetricsCollector` and takes a fast path with no measurable overhead. Hook calls are isolated — a collector that raises is logged and ignored, never turning a successful store or replay into a failure. Ships with `LoggingMetricsCollector` (one `DEBUG` line per operation) built in. `MetricsCollector`, `NoOpMetricsCollector`, and `LoggingMetricsCollector` are exported from `mcp_persist`.
- **RedisEventStore, SQLiteEventStore & PostgresEventStore**:
  - `create()` classmethod — an async context manager that owns the connection lifecycle, so callers no longer have to construct and tear down the underlying client/connection/pool themselves:
    ```python
    async with RedisEventStore.create("redis://localhost:6379", ttl=3600) as store:
        await store.store_event(...)
    ```
    `RedisEventStore.create(url, ...)` opens the client via `redis.asyncio.from_url`; `SQLiteEventStore.create(path, ...)` opens an `aiosqlite` connection and calls `initialize()`; `PostgresEventStore.create(dsn, ...)` opens an `asyncpg` pool and calls `initialize()`. The connection is always closed on exit, including when `initialize()` or the body raises. Store options (`ttl`, `key_prefix`/`table_name`, etc.) are keyword arguments; any extra keyword arguments are forwarded to the underlying driver (`from_url` / `connect` / `create_pool`). The driver is imported lazily inside `create()`, so importing `mcp_persist` still works without the optional backend installed.

## [1.2.1] - 2026-05-30

### Added
- **PostgresEventStore**:
  - `replay_batch_size` constructor parameter (default 500) to tune how many rows are fetched per round-trip during replay — useful for deployments with unusually large payloads, and previously only adjustable by monkey-patching a private module constant.
- **Documentation**:
  - "Redis connection pool sizing" section in `docs/production.md` explaining that `redis.asyncio` defaults to `max_connections=100` and raises `MaxConnectionsError` when the pool is exhausted (unlike `asyncpg`, which queues), with `max_connections` / `BlockingConnectionPool` remedies for high SSE fan-out.

## [1.2.0] - 2026-05-30

### Fixed
- **RedisEventStore, SQLiteEventStore & PostgresEventStore**:
  - `replay_events_after` no longer aborts the entire stream when it encounters a single event with a corrupt or unparseable payload. The offending event is now logged at `WARNING` and skipped, so a reconnecting client still receives every other event on the stream instead of losing the whole replay to one malformed row.

### Added
- **Tests**:
  - Regression tests for all three backends asserting that a corrupt payload injected mid-stream is skipped during replay while the events stored before and after it are still delivered.

## [1.1.4] - 2026-05-30

### Fixed
- **Tests**:
  - Add database and table cleanups (flushing Redis and dropping Postgres tables) at the end of the example integration tests to prevent state leakage to unit tests in CI environment.

### Added
- **Documentation**:
  - Code snippet in `docs/production.md` demonstrating python logging configuration details for all `mcp_persist.*` backends.

## [1.1.3] - 2026-05-30

### Changed
- **Tests**:
  - Parameterized example server integration test to run SQLite, Redis, and Postgres servers and check them with the client smoke test.
  - Automatically skip Redis and Postgres examples smoke tests locally when backend services are not running, while running them fully in CI.

## [1.1.2] - 2026-05-30

### Added
- **Tests**:
  - Integration smoke test that executes the full example SQLite server and verifies it against the client smoke test.

### Changed
- **Documentation**:
  - Reorganized benchmark results in the README into two clean tables showing storage performance and multi-scale replay performance separately.
  - Expanded production guide for explicit log configuration and SDK error monitoring of the `mcp_persist.*` loggers.

## [1.1.1] - 2026-05-30

### Fixed
- **Benchmarks**:
  - Code formatting standardizations for the benchmarks script.

## [1.1.0] - 2026-05-30

### Added
- **Benchmarks**:
  - Separate benchmarks measuring replay latency at multiple scales (100, 1,000, and 10,000 events) comparing SQLite, Redis, and Postgres.
- **Documentation**:
  - Dedicated "Architecture & Guarantees" section in the README explaining event ordering, concurrency/write semantics, and consistency/durability levels of each store.
  - Updated Redis replay comparison notes to reflect pipeline optimizations.

## [1.0.4] - 2026-05-30

### Added
- **PostgresEventStore & SQLiteEventStore**:
  - Double-quoted table and index names to allow hyphens and other valid non-standard SQL identifiers (e.g. `mcp-events`).
  - Validation pattern `^[a-zA-Z0-9_-]+$` replacing the Python-specific `isidentifier()` restriction.
- **SQLiteEventStore**:
  - In-process `self._init_lock` to serialize concurrent database first-time setup operations, consistent with the Postgres backend.
- **Tests**:
  - Stress tests in all backends that concurrently write to a stream and replay events from it, validating ordering and correctness under load.
- **Documentation**:
  - Simplified ASCII architecture diagram and earlier core purpose explanation in `README.md`.
  - Detailed production guide for Redis memory scaling under high stream cardinality (millions of unique stream IDs) and recommended eviction policies.
  - Documented system and software specs for the published benchmarks.

## [1.0.3] - 2026-05-30

### Added
- **RedisEventStore**:
  - `max_stream_length` constructor parameter to trim and bound the size of the stream's sorted set, preventing unbounded memory leak on active streams.
  - Normalization of Redis replies allowing full support for Redis clients configured with `decode_responses=True`.
- **PostgresEventStore & SQLiteEventStore**:
  - Index on `created_at` created during schema initialization to avoid sequential scans / table scans during `purge_expired()`.
  - Schema-qualified table name support (e.g. `schema.table` / `public.mcp_events`).
  - `timeout` constructor parameter for database busy/lock timeouts.
  - Streaming and batching during replay to avoid Out-Of-Memory (OOM) failures under massive event backlogs.

### Fixed
- **RedisEventStore**:
  - Pipeline switched to `transaction=False` to prevent `CROSSSLOT` execution failures on Redis Cluster environments.
  - Replay performance optimized by batching payload fetches into a single pipelined execute call rather than a sequential network loop.
  - Added lazy pruning of stale event IDs from the stream's sorted set during replay.
- **PostgresEventStore & SQLiteEventStore**:
  - Wrapped DDL creation queries in exception handlers that safely tolerate concurrent catalog registration races when multiple workers or replicas initialize at the same instant.

## [1.0.2] - 2026-05-30

### Fixed
- `replay_events_after` now returns `None` for a non-numeric `Last-Event-ID`
  instead of raising `ValueError`. The SDK passes this client-controlled header
  through unvalidated; previously SQLite and Postgres raised on `int()` (logging
  a traceback and aborting the replay) while Redis tolerated it. All three
  backends now handle it uniformly.
- Corrected the README and module-docstring quickstarts: they passed
  `app=mcp_server` (undefined, and the wrong type) — `StreamableHTTPSessionManager`
  needs the low-level server, so they now show `app=mcp._mcp_server` from a
  `FastMCP` instance, matching the runnable examples.

### Added
- `docs/production.md` — a production deployment guide (scheduling
  `purge_expired()`, failure modes, schema and permissions, security, scaling,
  observability), linked from the README.

### Changed
- Removed the redundant `aiosqlite` line from the SQLite example's install
  instructions (the `[sqlite]` extra already provides it).

### Tests
- Added a non-numeric `Last-Event-ID` replay test to all three backend suites.

## [1.0.1] - 2026-05-30

### Fixed
- `RedisEventStore` no longer sets a TTL on the counter key. Previously the
  counter expired along with events, so after an idle period longer than `ttl`
  the next event ID restarted from `1`, breaking the monotonic-ID guarantee. The
  counter now persists for the life of the Redis instance, matching the
  `AUTOINCREMENT` / `IDENTITY` sequences of the SQLite and Postgres backends.
- `PostgresEventStore.initialize` is now guarded by a lock and short-circuits
  once initialized, so concurrent first `store_event` calls on a pool can no
  longer race on `CREATE TABLE IF NOT EXISTS` (which can raise a duplicate-key
  error on Postgres system catalogs).

### Added
- `mcp_persist.__version__`, resolved from installed package metadata.

### Changed
- `RedisEventStore.store_event` now writes the event hash, its sorted-set entry,
  and their TTLs in a single transactional pipeline instead of separate
  round-trips. This makes the per-event write atomic — a mid-write crash can no
  longer orphan an event hash or leave a key without its expiry — and removes
  round-trips from the hot path.
- README: explained SQLite's throughput advantage (no network hop) and its
  single-writer caveat; surfaced the Redis per-event replay cost (`O(log N + M)`,
  one round-trip per replayed event) in the `RedisEventStore` "How it works"
  section; spelled out the consequence of multi-process SQLite access
  (`SQLITE_BUSY` / "database is locked").
- Documentation: added `PostgresEventStore` to the `CONTRIBUTING.md` intro, the
  bug-report issue template, and the pull-request checklist.

### Tests
- Corrected the Redis counter-TTL test to assert the counter has no expiry
  (was asserting the buggy behavior), clarified that the SQLite concurrent-ID
  test passes only because aiosqlite serializes writes through one connection,
  and added a package-level test covering `__version__` and the public exports.

## [1.0.0] - 2026-05-27

First stable release. The three backends (`RedisEventStore`, `SQLiteEventStore`,
`PostgresEventStore`) and their public API are now considered stable; future
breaking changes will follow semantic versioning with a major version bump.

### Added
- `benchmarks/benchmark.py` comparing `store_event` latency/throughput and
  `replay_events_after` latency across all three backends.
- "Choosing a backend" section in the README with a decision guide and
  comparison table, plus a benchmarks summary.

### Changed
- Clarified the `PostgresEventStore.purge_expired` docstring with a one-line
  explanation of `pg_cron`.

## [0.3.0] - 2026-05-27

### Added
- `PostgresEventStore` — PostgreSQL-backed `EventStore` (via `asyncpg`) for
  durable SSE resumability on deployments already running Postgres, including
  multi-node / team setups. Install with the `postgres` extra.
- Example MCP server `examples/postgres_server.py`.
- `py.typed` marker so downstream type checkers use the bundled type hints (PEP 561).

### Removed
- The published `dev` extra. Development dependencies now live in a PEP 735
  `[dependency-groups]` table, so `pip install "mcp-persist[dev]"` is no longer
  available; contributors use `uv sync --dev` instead. The `redis` and `sqlite`
  extras are unchanged.

## [0.2.0] - 2026-05-27

### Added
- `SQLiteEventStore` — SQLite-backed `EventStore` for single-node SSE
  resumability that survives process restarts, with no external service.
- Example MCP servers for both backends under `examples/`.

## [0.1.1] - 2026-05-26

### Fixed
- Broken import that made the package unimportable on current `mcp` releases.

### Changed
- Restored the `src/` layout.

## [0.1.0] - 2026-05-26

### Added
- Initial release with `RedisEventStore` — Redis-backed `EventStore` for
  multi-worker / multi-process SSE resumability.

[Unreleased]: https://github.com/Ar-maan05/mcp-persist/compare/v1.7.0...HEAD
[1.7.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/Ar-maan05/mcp-persist/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.4...v1.2.0
[1.1.4]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.3...v1.1.4
[1.1.3]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.4...v1.1.0
[1.0.4]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/Ar-maan05/mcp-persist/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/Ar-maan05/mcp-persist/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Ar-maan05/mcp-persist/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/Ar-maan05/mcp-persist/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Ar-maan05/mcp-persist/releases/tag/v0.1.0
