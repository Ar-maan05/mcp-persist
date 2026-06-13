# Benchmarks

[`benchmarks/benchmark.py`](../benchmarks/benchmark.py) measures `store_event`
latency (sequential), `store_event` throughput (concurrent), and
`replay_events_after` latency across all three backends. SQLite runs against an
on-disk file (its realistic durable mode), and Redis/Postgres run over the
network. Run it yourself:

```bash
uv run python benchmarks/benchmark.py --events 5000 --concurrency 500
```

> **These numbers are indicative, not authoritative.** Absolute latency and
> throughput depend heavily on hardware, disk, network, and server tuning. Run the script in *your* environment for numbers that matter.

## Benchmark Environment Spec
The table below was measured with the following configuration:
- **CPU / Machine:** AMD Ryzen AI 7 350 (8 cores, 16 threads), 24GB DDR5 5600, PCIe Gen 5 NVMe SSD storage, running Fedora Linux 44 (Workstation Edition) x86_64
- **Python Version:** 3.12.2
- **Redis Version:** 8.8.0 (container on localhost)
- **PostgreSQL Version:** 18.4 (container on localhost)

Measured with `--events 5000 --concurrency 500`:

### Storage Performance

| Backend | store p50 | store p95 | store mean | store throughput |
|---|---|---|---|---|
| SQLite | 57.2 µs | 78.4 µs | 61.6 µs | 23,517 ev/s |
| Redis | 65.6 µs | 93.1 µs | 73.7 µs | 7,857 ev/s |
| Postgres | 626.1 µs | 913.4 µs | 660.0 µs | 7,427 ev/s |

### Replay Performance (Total Latency)

| Backend | Replay 100 | Replay 1,000 | Replay 10,000 |
|---|---|---|---|
| SQLite | 0.93 ms | 6.51 ms | 27.41 ms |
| Redis | 1.00 ms | 8.79 ms | 76.08 ms |
| Postgres | 2.96 ms | 6.58 ms | 61.13 ms |

What the shape of these results reflects (and should hold across environments):

- **SQLite has the lowest latency _and_ the highest throughput**: it runs
  in-process with no network hop, so every `store_event` skips a round-trip
  entirely. The catch is that it's single-writer: that throughput doesn't scale
  across processes, which is why multi-worker deployments still reach for Redis
  or Postgres despite the lower single-node numbers.
- **Redis and Postgres pay a network round-trip per store**, so per-call latency
  is higher than SQLite. The two land at comparable throughput (~7,400–7,900
  ev/s at concurrency 500) for opposite reasons: Redis has low per-call latency
  but every write serializes through the single `INCR` counter (see the write
  ceiling note in [architecture.md](architecture.md#2-concurrency--write-semantics)), while Postgres has much higher per-call latency but its
  pooled connections run many stores concurrently.
- **Replay**: SQLite and Postgres fetch a stream's events in one indexed query, while the Redis backend issues a `zrangebyscore` followed by a single pipelined execution to fetch payloads concurrently, keeping the entire replay latency bounded to exactly two network round-trips.
