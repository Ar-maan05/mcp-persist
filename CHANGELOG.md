# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.1...HEAD
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
