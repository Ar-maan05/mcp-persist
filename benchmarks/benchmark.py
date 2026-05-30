"""Benchmark the mcp-persist backends: store_event and replay_events_after.

Measures, per backend:
  - store latency      — sequential store_event calls (mean / p50 / p95, microseconds)
  - store throughput   — concurrent store_event calls (events/second)
  - replay latency     — time to replay a stream of N events (total ms + per-event us)

SQLite is benchmarked against an on-disk file (its realistic durable mode), not
:memory:, so the comparison reflects how each backend is actually deployed.

Backends are included only if reachable:
  - SQLite   — always (temp file)
  - Redis    — MCP_TEST_REDIS_URL or redis://localhost:6379/0
  - Postgres — MCP_TEST_POSTGRES_URL or postgresql://postgres@localhost:5432/postgres

Usage:
    uv run python benchmarks/benchmark.py
    uv run python benchmarks/benchmark.py --events 5000 --concurrency 100
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import statistics
import tempfile
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from mcp.types import JSONRPCRequest

from mcp_persist import PostgresEventStore, RedisEventStore, SQLiteEventStore

SAMPLE = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")

REDIS_URL = os.environ.get("MCP_TEST_REDIS_URL", "redis://localhost:6379/0")
POSTGRES_URL = os.environ.get("MCP_TEST_POSTGRES_URL", "postgresql://postgres@localhost:5432/postgres")

TTL = 3600  # all backends configured identically for a fair comparison


def _us(seconds: float) -> float:
    return seconds * 1_000_000


async def _noop(_event: object) -> None:
    pass


async def bench_store_sequential(store, n: int) -> dict[str, float]:
    """Per-call latency for sequential store_event."""
    latencies: list[float] = []
    for _ in range(n):
        start = time.perf_counter()
        await store.store_event("seq-stream", SAMPLE)
        latencies.append(time.perf_counter() - start)
    latencies.sort()
    return {
        "mean_us": _us(statistics.fmean(latencies)),
        "p50_us": _us(latencies[len(latencies) // 2]),
        "p95_us": _us(latencies[int(len(latencies) * 0.95)]),
    }


async def bench_store_throughput(store, n: int, concurrency: int) -> float:
    """Events/second storing n events with bounded concurrency."""
    sem = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with sem:
            await store.store_event("tput-stream", SAMPLE)

    start = time.perf_counter()
    await asyncio.gather(*(one() for _ in range(n)))
    elapsed = time.perf_counter() - start
    return n / elapsed


async def bench_replay(store, n: int) -> dict[str, float]:
    """Time to replay a stream of n events."""
    anchor = await store.store_event("replay-stream", SAMPLE)
    for _ in range(n):
        await store.store_event("replay-stream", SAMPLE)

    count = 0

    async def count_cb(_event: object) -> None:
        nonlocal count
        count += 1

    start = time.perf_counter()
    await store.replay_events_after(anchor, count_cb)
    elapsed = time.perf_counter() - start
    assert count == n, f"expected {n} replayed events, got {count}"
    return {"total_ms": elapsed * 1000, "per_event_us": _us(elapsed) / n}


# ── Backend setup (each yields a ready store; skips if unreachable) ─────────────


@contextlib.asynccontextmanager
async def sqlite_store() -> AsyncIterator[SQLiteEventStore]:
    import aiosqlite

    tmp = Path(tempfile.mkdtemp()) / "bench.db"
    conn = await aiosqlite.connect(str(tmp))
    try:
        store = SQLiteEventStore(conn, ttl=TTL)
        await store.initialize()
        yield store
    finally:
        await conn.close()
        tmp.unlink(missing_ok=True)


@contextlib.asynccontextmanager
async def redis_store() -> AsyncIterator[RedisEventStore]:
    import redis.asyncio as aioredis

    client = aioredis.from_url(REDIS_URL)
    await client.flushdb()
    try:
        yield RedisEventStore(client, ttl=TTL)
    finally:
        await client.flushdb()
        await client.aclose()


@contextlib.asynccontextmanager
async def postgres_store() -> AsyncIterator[PostgresEventStore]:
    import asyncpg

    pool = await asyncpg.create_pool(POSTGRES_URL)
    try:
        await pool.execute("DROP TABLE IF EXISTS bench_events")
        store = PostgresEventStore(pool, table_name="bench_events", ttl=TTL)
        await store.initialize()
        yield store
        await pool.execute("DROP TABLE IF EXISTS bench_events")
    finally:
        await pool.close()


BACKENDS = {
    "SQLite": sqlite_store,
    "Redis": redis_store,
    "Postgres": postgres_store,
}


async def run_backend(name: str, factory, events: int, concurrency: int) -> dict[str, Any] | None:
    try:
        async with factory() as store:
            # Warm up so connection/pool/cache costs don't skew the first sample.
            for _ in range(50):
                await store.store_event("warmup", SAMPLE)

            seq = await bench_store_sequential(store, events)
            tput = await bench_store_throughput(store, events, concurrency)

            # Benchmark replay at multiple scales
            replay_100 = await bench_replay(store, 100)
            replay_1000 = await bench_replay(store, 1000)
            replay_10000 = await bench_replay(store, 10000)

            return {
                **seq,
                "throughput_eps": tput,
                "replay_100_ms": replay_100["total_ms"],
                "replay_1000_ms": replay_1000["total_ms"],
                "replay_10000_ms": replay_10000["total_ms"],
            }
    except Exception as exc:  # unreachable backend, missing driver, etc.
        print(f"  {name}: skipped ({type(exc).__name__}: {exc})")
        return None


def print_table(results: dict[str, dict[str, Any]]) -> None:
    if not results:
        print("\nNo backends were reachable.")
        return

    # Table 1: Storage Performance
    header1 = f"{'Backend':<10} {'store p50':>12} {'store p95':>12} {'store mean':>12} {'throughput':>15}"
    print("\nStorage Performance:")
    print("-" * len(header1))
    print(header1)
    print("-" * len(header1))
    for name, r in results.items():
        print(
            f"{name:<10} "
            f"{r['p50_us']:>9.1f} us {r['p95_us']:>9.1f} us {r['mean_us']:>9.1f} us "
            f"{r['throughput_eps']:>10,.0f} ev/s"
        )
    print("-" * len(header1))

    # Table 2: Replay Latency
    header2 = f"{'Backend':<10} {'Replay 100':>15} {'Replay 1,000':>15} {'Replay 10,000':>15}"
    print("\nReplay Performance (Total Latency):")
    print("-" * len(header2))
    print(header2)
    print("-" * len(header2))
    for name, r in results.items():
        print(
            f"{name:<10} "
            f"{r['replay_100_ms']:>12.2f} ms {r['replay_1000_ms']:>12.2f} ms {r['replay_10000_ms']:>12.2f} ms"
        )
    print("-" * len(header2))


async def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark mcp-persist backends.")
    parser.add_argument("--events", type=int, default=2000, help="events per phase (default: 2000)")
    parser.add_argument("--concurrency", type=int, default=50, help="concurrent stores for throughput (default: 50)")
    args = parser.parse_args()

    print(f"Benchmarking {args.events} events, concurrency {args.concurrency}")
    results: dict[str, dict[str, Any]] = {}
    for name, factory in BACKENDS.items():
        result = await run_backend(name, factory, args.events, args.concurrency)
        if result is not None:
            results[name] = result

    print_table(results)


if __name__ == "__main__":
    asyncio.run(main())
