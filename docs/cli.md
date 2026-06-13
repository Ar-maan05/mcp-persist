# Command-line tools

`mcp-persist` ships two diagnostic commands for operating a live store. Both
resolve their target the same way as the proxy: explicit `--backend`/`--url`
flags, or the `MCP_PERSIST_*` environment variables when no flags are given.

- [`mcp-persist doctor`](#mcp-persist-doctor) — pass/fail health checklist
- [`mcp-persist stats`](#mcp-persist-stats) — per-stream event inventory

## `mcp-persist doctor`

Before you debug a deployment, run the doctor. It is a pass/fail checklist for the
things that usually explain a broken or silently growing store: the Python
runtime, whether the backend's driver extra is installed, live connectivity, and
config that lets events accumulate without bound.

```bash
# Check a specific store:
mcp-persist doctor --backend sqlite --url events.db --ttl 3600

# …or check whatever MCP_PERSIST_* is configured (no flags needed):
mcp-persist doctor

# Machine-readable, for CI or a readiness gate:
mcp-persist doctor --json
```

```text
mcp-persist doctor: redis (redis://localhost:6379)

[ ok ] python        Python 3.12.13 (>= 3.10)
[ ok ] driver        redis is installed for the redis backend
[ ok ] connectivity  connected to redis (redis 7.2.0)
[warn] retention     ttl is not set: events accumulate in Redis indefinitely; set --ttl

All checks passed with 1 warning(s).
```

The runtime, driver, and retention checks read your resolved config, so they run
even when the backend is unreachable (exactly when you reach for the doctor); a
store that will not open is reported as a failed `connectivity` check rather than
a crash. The command exits non-zero only when a check **fails**; warnings (an
unset `ttl`, for example) are surfaced but do not fail the run, so a warning will
not break a CI gate that treats exit code as health.

## `mcp-persist stats`

`mcp-persist stats` reports how many events each stream holds, their event ID
range, and a latency probe timed against the backend's native `PING` / `SELECT 1`.
It reads the store directly (a single `ZCARD`/`ZRANGE` pass on Redis, one
`GROUP BY stream_id` on SQLite/Postgres), so it is cheap to run against a live
deployment.

```bash
# Every stream, plus totals and a latency probe:
mcp-persist stats --backend sqlite --url events.db

# A single stream:
mcp-persist stats --backend redis --url redis://localhost:6379 --stream-id session-42:_GET_stream

# JSON for scripting / dashboards:
mcp-persist stats --json
```

```text
mcp-persist stats: sqlite (events.db)

stream                   events  min  max
session-a:_GET_stream        12    1   12
session-b:notifications       5   13   17

2 stream(s), 17 event(s), last id 17, ping 0.11 ms
```

`last id` is the latest event ID assigned: the never-expired counter on Redis, or
the highest stored ID on SQLite/Postgres (which can trail the sequence once old
rows are purged). Config is resolved exactly like the proxy and `doctor`
(`--backend`/`--url` or `MCP_PERSIST_*`). An unreachable store prints a single
error line and exits non-zero rather than a traceback.
