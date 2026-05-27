# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/Ar-maan05/mcp-persist/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/Ar-maan05/mcp-persist/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Ar-maan05/mcp-persist/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/Ar-maan05/mcp-persist/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Ar-maan05/mcp-persist/releases/tag/v0.1.0
