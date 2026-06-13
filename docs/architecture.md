# Architecture & Guarantees

This document outlines the consistency, ordering, and concurrency guarantees of
`mcp-persist` backends. For operational guidance (sizing, failure modes,
deployment topologies), see [production.md](production.md).

## 1. Event Ordering
- **Per-Stream vs Global:** All backends guarantee that event IDs are monotonically increasing, representing a sequential log of events. However, because client-side replay request handling relies on range scans queryable by stream ID, ordering guarantees are **per-stream**.
- **Preserved Order:** Outbound events written to a specific stream via `store_event` are guaranteed to be replayed in the exact order they were written.

## 2. Concurrency & Write Semantics
- **Concurrent Writes:**
  - **Redis:** `store_event` increments a global atomic counter via `INCR` to get the next sequential ID, and then pipelined commands write the event hash and add it to the stream's sorted set. Multiple workers can write concurrently without any locking, and the IDs are guaranteed to be unique and monotonically increasing.
  - **SQLite:** SQLite is single-writer and serializes all writes. `aiosqlite` uses an in-process thread pool to queue commands on a single connection. Concurrent writes from multiple processes are not supported and will raise `SQLITE_BUSY` errors.
  - **PostgreSQL:** Uses a native `BIGINT GENERATED ALWAYS AS IDENTITY` column which handles concurrent sequence increments safely across database sessions.
- **Duplicate Event IDs:** Duplicate event IDs are structurally impossible. All backends rely on atomic database counters (`AUTOINCREMENT` for SQLite, `IDENTITY` for Postgres, and `INCR` for Redis) which generate strictly unique and non-overlapping sequence numbers.
- **Redis counter as the write ceiling:** Because every write `INCR`s a single `{prefix}counter` key, all writes serialize through that one key. On a single Redis it is rarely the bottleneck, but on **Redis Cluster** the counter lives on one shard, setting the aggregate write-throughput ceiling regardless of cluster size. See [production.md](production.md#redis-monotonic-counter-throughput-ceiling) for benchmarking guidance.

## 3. Consistency & Durability
- **SQLite:** Configured with WAL (`Write-Ahead Logging`) journaling. Writes are flushed to disk on commit, ensuring durability across process restarts.
- **Postgres:** Fully ACID compliant. Events are durable once the transaction commits.
- **Redis:** Relies on Redis persistence configuration (RDB/AOF). If Redis is deployed as a cache (with no persistence) or with lazy AOF flushing, a Redis crash could roll back the database state, potentially repeating or dropping IDs. For strong durability, configure Redis with AOF (`appendfsync everysec` or `always`).
