# Command-line tools

`mcp-persist` ships administrative commands for operating a live store. They
resolve their target the same way as the proxy: explicit non-secret flags or the
`MCP_PERSIST_*` environment variables. In particular, every command honors
tenant binding, compression, and `MCP_PERSIST_ENCRYPTION_*`, so a dump or load
uses the same scoped, encrypted store as the running application. Encryption
keys intentionally remain environment-only and are never accepted as CLI flags.

- [`mcp-persist config`](#mcp-persist-config): show the resolved store settings
- [`mcp-persist doctor`](#mcp-persist-doctor): pass/fail health checklist
- [`mcp-persist stats`](#mcp-persist-stats): per-stream event inventory
- [`mcp-persist purge`](#mcp-persist-purge): force a purge of expired events
- [`mcp-persist migrate`](#mcp-persist-migrate): copy events between backends
- [`mcp-persist-proxy --check`](#mcp-persist-proxy---check): upstream pre-flight probe

## `mcp-persist config`

`config` answers the question that precedes every other one: which store am I
actually pointed at? It prints the settings resolved from `MCP_PERSIST_*` and any
flags, without opening a connection, so it works against a store that is down.

```bash
mcp-persist config
mcp-persist config --json          # machine-readable
```

```text
mcp-persist config: resolved from MCP_PERSIST_* and command-line flags

  backend            postgres
  url                postgresql://app:***@db.internal:5432/events
  ttl                3600s
  table_name         default
  key_prefix         default
  max_stream_length  unset
  tenant_id          team-a
  compression        zstd
  encryption         on (active key 'k2', 2 key(s) available)
```

Nothing secret is printed: a password in the URL is masked, and encryption is
reported as the active key id and how many keys the ring can decrypt with (key
ids travel in the payload marker and are not sensitive), never the keys. Reach
for it when a command reports an empty store or the wrong tenant; the usual cause
is an environment variable set somewhere you forgot.

## `mcp-persist doctor`

Before you debug a deployment, run the doctor. It is a pass/fail checklist for the
things that usually explain a broken or silently growing store: the Python
runtime, whether the backend's driver extra is installed, live connectivity,
config that lets events accumulate without bound, and whether a configured
compression codec or encryption keyring is actually usable.

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
[ ok ] compression   compression is disabled
[ ok ] encryption    encryption is disabled

All checks passed with 1 warning(s).
```

The `compression` check runs the same guard the stores run at construction, so
`MCP_PERSIST_COMPRESSION=zstd` without the `zstd` extra (or an unknown codec)
fails here with the pip hint instead of at the first write. The `encryption`
check parses `MCP_PERSIST_ENCRYPTION_*` and fails on a malformed key set, or on a
keyring configured while the `crypto` extra is not installed (the keyring builds
without `cryptography`, so that gap otherwise stays silent until a write).

Both CLIs also accept `--version`, which prints the installed `mcp-persist`
version and exits.

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

```bash
# Delete by an explicit age instead of the ttl (also 12h, 45m, 3600s, 2w, or a
# bare number of seconds). Works even when no ttl is configured:
mcp-persist purge --older-than 30d

# Count what a 30-day cutoff would remove, without deleting:
mcp-persist purge --older-than 30d --dry-run
```

`--older-than` deletes events older than the given duration regardless of the
configured `ttl`, useful for a one-off cleanup or a store that keeps events
indefinitely by default. It shares the same efficient bulk and `--batch-size`
`DELETE` paths as the ttl-based purge. Supported on the SQLite and Postgres
backends; Redis expires keys natively and rejects the flag.

## `mcp-persist dump` and `mcp-persist load`

`mcp-persist dump <stream>` exports a single stream's events to a portable,
versioned JSON document; `mcp-persist load` reads one back into a store. The
intended use is bug reports and test fixtures: capture a failing session and
replay it into a fresh store to reproduce.

```bash
# Export one stream to a file (omit -o to write JSON to stdout):
mcp-persist dump session-abc123 --backend sqlite --url events.db -o session.json

# Load it into a fresh store (reads stdin when no path is given):
mcp-persist load session.json --backend sqlite --url repro.db

# Restore under a different stream name:
mcp-persist load session.json --stream-id replayed
```

The document is `{"format": "mcp-persist-dump", "version": 1, "stream_id": ...,
"events": [...]}`, with each event as `{"event_id": ..., "message": {...}}` (a
priming event is `{"message": null}`). Payloads are exported as decompressed,
decrypted plaintext regardless of how the source store persists them, and `load`
validates the envelope before writing anything, failing closed on an
unrecognized format or version. As with `migrate`, `load` re-stores each event
with `store_event`, so the destination assigns fresh IDs: content and ordering
are reproduced, not the original resumability tokens. Both subcommands are thin
front ends to `export_stream()` / `import_stream()` (see `docs/api.md`).

Use `--tenant-id`, `--key-prefix`, `--max-stream-length`, or `--compression` to
override the matching non-secret environment setting for one invocation. Keys
continue to come from `MCP_PERSIST_ENCRYPTION_*`.

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

### Configuring both stores

`migrate` names two stores, so every store setting has a `--from-` and a `--to-`
form: `--from-ttl`, `--from-table`, `--from-key-prefix`,
`--from-max-stream-length`, `--from-tenant-id`, `--from-compression`, and the
matching `--to-` flags. Each falls back to its `MCP_PERSIST_*` variable when the
flag is absent, so migrating the deployment your shell is already configured for
needs no extra flags; spell out the per-side flags when the two ends are
configured differently.

```bash
# Move one tenant out of a shared SQLite store into its own Postgres database:
mcp-persist migrate \
    --from-backend sqlite   --from-url shared.db --from-tenant-id team-a \
    --to-backend   postgres --to-url postgresql://localhost/team_a
```

A source that is scoped, uses a non-default table or key prefix, or is encrypted
must be described accurately or `migrate` reads the wrong rows: an unscoped read
of a multi-tenant store merges every tenant into the destination, and a
mis-specified table simply finds nothing.

Encryption keys stay environment-only, as everywhere else, and one keyring covers
both sides. That is enough to re-key during the move: put the source's old key
and the destination's new key in `MCP_PERSIST_ENCRYPTION_KEYS` and point
`MCP_PERSIST_ENCRYPTION_KEY_ID` at the new one, so events are read with the old
key and written with the new.

### Events the source cannot read

A store skips an event it can read from storage but cannot decode (the usual
cause is a payload it has no key for; the rest is genuine corruption) rather than
aborting the whole migration. Those events are counted and reported:

```text
mcp-persist: error: skipped 2 event(s) the source could not decode; they were NOT
copied. If the source store is encrypted, set MCP_PERSIST_ENCRYPTION_KEY(S) to
its key and run again.
```

Any non-zero skip count is an error and exits non-zero, and appears as
`skipped_events` under `--json`. Treat it as a failed migration: the destination
is missing those events. The usual fix is to configure the source's keyring and
run again.

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
