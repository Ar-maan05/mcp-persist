# Command-line tools

`mcp-persist` ships two diagnostic commands for operating a live store. Both
resolve their target the same way as the proxy: explicit `--backend`/`--url`
flags, or the `MCP_PERSIST_*` environment variables when no flags are given.

- [`mcp-persist doctor`](#mcp-persist-doctor): pass/fail health checklist
- [`mcp-persist stats`](#mcp-persist-stats): per-stream event inventory
- [`mcp-persist purge`](#mcp-persist-purge): force a purge of expired events
- [`mcp-persist migrate`](#mcp-persist-migrate): copy events between backends
- [`mcp-persist-proxy --check`](#mcp-persist-proxy---check): upstream pre-flight probe

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

## `mcp-persist purge`

`mcp-persist purge` forces an immediate `purge_expired()` against the configured
store and reports how many events it removed. It is the on-demand counterpart to
the in-process `PurgeScheduler`, useful for a cron job or a one-off cleanup.

```bash
# Delete every expired event now:
mcp-persist purge --backend postgres --url postgresql://localhost/app --ttl 3600

# Delete in bounded chunks so one long DELETE does not contend with live writes:
mcp-persist purge --batch-size 1000

# Count what would be deleted without touching anything:
mcp-persist purge --dry-run
```

`--dry-run` reports the expired count via `count_expired()` and deletes nothing.
Purge is tenant-scoped when `MCP_PERSIST_TENANT_ID` is set. A store configured
without a `ttl` purges nothing (there is no expiry to act on).

## `mcp-persist migrate`

`mcp-persist migrate` copies every stream from one backend to another, the CLI
front end to the `migrate()` function. Use it to move a deployment between
backends (for example SQLite to Postgres) or to seed a cold archive store.

```bash
mcp-persist migrate \
    --from-backend sqlite   --from-url events.db \
    --to-backend   postgres --to-url   postgresql://localhost/app \
    --batch-size 500
```

It prints one line per stream as it goes (or `--json` for a machine-readable
summary) and exits non-zero if any stream failed. Payloads and ordering are
preserved; as with `migrate()`, the destination assigns fresh event IDs, so
in-flight resumability tokens are invalidated by the move (reconnecting clients
start a fresh stream). Run it during a maintenance window.

## `mcp-persist-proxy --check`

Before committing to a long-running proxy, `--check` probes the upstream and
exits. It is a fast pre-flight that catches the two mistakes that otherwise only
surface once clients connect: an upstream that is down, and a wrong `--path` (or
a host that is not an MCP server at all). It requires `--upstream` (a running
server in mode 1); it is not meaningful before a subprocess upstream has started.

```bash
mcp-persist-proxy --upstream http://localhost:8001 --check
# narrow the endpoint path if your server does not serve /mcp:
mcp-persist-proxy --upstream http://localhost:8001 --path /api/mcp --check
```

```text
mcp-persist-proxy check: http://localhost:8001/mcp

[ ok ] reachable       upstream responded (HTTP 200)
[ ok ] streamable-http upstream speaks MCP Streamable HTTP (text/event-stream)

Upstream looks ready to proxy.
```

Two honest levels are reported:

- **reachable**: an HTTP connection to the endpoint succeeds. A connection error
  fails here and stops, since nothing else is knowable.
- **streamable-http**: a minimal MCP `initialize` POST comes back looking like
  Streamable HTTP, either a `text/event-stream` response or a JSON-RPC body. A
  404/405 is a failure with a hint to check `--path`; any other non-MCP response
  is a warning (the host answered but does not look like an MCP server).

The command exits non-zero when a level **fails**; a warning does not fail it, so
"reachable but not obviously MCP" still lets you proceed. No event store is opened
during a check, so it never touches Redis or Postgres.
