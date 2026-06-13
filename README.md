# mcp-persist

[![CI](https://github.com/Ar-maan05/mcp-persist/actions/workflows/ci.yml/badge.svg)](https://github.com/Ar-maan05/mcp-persist/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/mcp-persist.svg)](https://pypi.org/project/mcp-persist/)
[![Downloads](https://static.pepy.tech/badge/mcp-persist)](https://pepy.tech/project/mcp-persist)
[![Python versions](https://img.shields.io/pypi/pyversions/mcp-persist.svg)](https://pypi.org/project/mcp-persist/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

When an MCP client reconnects, the server has to replay the events it missed, and with only the SDK's in-memory `EventStore`, that replay is impossible: the session lived in one process's memory, so a restart or a reconnect to a different worker loses it. **`mcp-persist` adds drop-in durable `EventStore` backends for SQLite, Redis, and PostgreSQL** that survive process restarts and scale across multi-worker deployments, keeping SSE stream resumability intact.

> 📚 This README is the quick tour. Full reference lives in **[`docs/`](#-documentation)** — backends, CLI, the programmatic API, architecture, benchmarks, and the production guide.

## Quickstart: `with_persistence()`

The fastest way to add resumability to a FastMCP server. Wiring it by hand means
an event store, a `StreamableHTTPSessionManager`, a Starlette lifespan to open and
close them, and a `Mount`. `with_persistence()` collapses all of it to two lines:
pass your `FastMCP` instance and get back a runnable Starlette ASGI app with the
store and session manager already wired in, opened on startup and closed on
shutdown.

```python
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp_persist import with_persistence

mcp = FastMCP(name="MyServer")

# Swap backend="redis" / "postgres" with the matching url:
app = with_persistence(mcp, backend="sqlite", url="events.db", ttl=3600)
uvicorn.run(app, host="127.0.0.1", port=8000)  # MCP endpoint at /mcp
```

That replaces ~35 lines of manual lifespan/`Mount`/session-manager boilerplate.
There are three ways to supply the store, resolved in order:

```python
# A: config kwargs; the app builds the store and owns its lifecycle:
app = with_persistence(mcp, backend="redis", url="redis://localhost:6379", ttl=3600)

# B: bring your own store; you own its lifecycle (the app does NOT close it):
async with SQLiteEventStore.create("events.db", ttl=3600) as store:
    app = with_persistence(mcp, store=store)
    await uvicorn.Server(uvicorn.Config(app, port=8000)).serve()

# C: configure from the environment (MCP_PERSIST_BACKEND / _URL / _TTL / …):
app = with_persistence(mcp)
```

The live store is exposed on `app.state.event_store`, so you can run a
[`PurgeScheduler`](docs/api.md#scheduled-cleanup-purgescheduler) alongside the
server. No extra dependency is required: `starlette` and the session manager ship
with `mcp`. See [`examples/fastmcp_plugin_server.py`](examples/fastmcp_plugin_server.py).

Under the hood, whichever setup you use, it's the same layering: a
`StreamableHTTPSessionManager` backed by a durable `EventStore` you choose.

```
MCP Server
     │
     ▼
StreamableHTTPSessionManager
     │
     ▼
EventStore
 ├─ SQLite
 ├─ Redis
 └─ PostgreSQL
```

> **Not on FastMCP, or want to own the wiring yourself?** Build a store and pass
> it to `StreamableHTTPSessionManager` directly — see
> [Manual wiring](docs/backends.md#manual-wiring-advanced-or-non-fastmcp).

## Resumability without touching the server: `PersistenceProxy`

When you can't (or don't want to) modify the MCP server, such as a third-party server,
another language, or a binary you don't own, run the **proxy** in front of it. It
forwards requests upstream and intercepts the SSE responses, persisting every
event to a store and assigning its own event IDs. A client that disconnects
reconnects with `Last-Event-ID`; the proxy replays the missed events from the
store and continues live. The upstream needs **no** event store of its own: the
proxy is the store.

> **Running a TypeScript (or any non-Python) MCP server?** The proxy speaks plain
> HTTP, so it adds resumability in front of it without touching the server. See
> [docs/typescript.md](docs/typescript.md) for a step-by-step guide.

Point your clients at the proxy's address instead of the server's (e.g.
`http://localhost:8000/mcp`); nothing else on the client changes. Resumability
rides the standard SSE `Last-Event-ID` header, so any MCP client that reconnects
after a drop gets its missed events back automatically.

```bash
# Point at a running MCP server (no extra install needed: httpx & uvicorn
# already ship with mcp):
mcp-persist-proxy --upstream http://localhost:8001 \
    --backend sqlite --url events.db --port 8000

# …or start the server as a subprocess, wait for it, and proxy it:
mcp-persist-proxy --backend redis --url redis://localhost:6379 \
    --port 8000 --upstream-port 8001 -- uvicorn my_server:app --port 8001
```

Or embed it as an ASGI app:

```python
import uvicorn
from mcp_persist import PersistenceProxy

async def serve():
    async with PersistenceProxy.create(
        "http://localhost:8001", backend="sqlite", url="events.db", ttl=3600
    ) as proxy:
        await uvicorn.Server(uvicorn.Config(proxy, port=8000)).serve()
```

The store is resolved exactly like [`with_persistence`](#quickstart-with_persistence):
a pre-built `store=`, `backend=`+`url=`, or `MCP_PERSIST_*` env vars. (`ttl` is
how long stored events are kept, in seconds; it's available as `--ttl` on the CLI
too.)

**What it does and does not do.** It adds resumability against a *stable
upstream*: a server that stays up while clients come and go. It survives client
disconnects (flaky networks, mobile, tunnels), and, with a durable store like
SQLite or Postgres, a restart of the proxy itself. Two things it can't do: it
can't recover from the **upstream server** restarting: a restarted server is a
clean break, so the proxy can replay what it already stored but can't carry the
old connection over to the new server; and it can't replay an event that was
never stored: if the client and the proxy both drop before an event is saved,
it's gone. It never makes delivery less reliable than talking to the server
directly.

## Command-line tools

Two diagnostic commands for operating a live store. Both resolve their target
from `--backend`/`--url` flags or the `MCP_PERSIST_*` env vars. Full reference,
sample output, JSON schema, and exit-code semantics in
**[docs/cli.md](docs/cli.md)**.

```bash
# Pass/fail health checklist (runtime, driver, connectivity, retention):
mcp-persist doctor --backend sqlite --url events.db --ttl 3600

# Per-stream event inventory + latency probe:
mcp-persist stats --backend sqlite --url events.db
```

## Backends & choosing one

| Backend | Extra | Use case |
|---|---|---|
| `SQLiteEventStore` | `sqlite` | Single-process SSE resumability across restarts, with no external service |
| `RedisEventStore` | `redis` | Multi-process / multi-worker SSE resumability |
| `PostgresEventStore` | `postgres` | Durable resumability for deployments already running Postgres, including multi-node / team setups |

Start from how you deploy:

| If your deployment… | Use |
|---|---|
| Runs as a single process and you want zero extra infrastructure | `SQLiteEventStore` |
| Runs multiple workers / replicas behind a load balancer | `RedisEventStore` |
| Already runs PostgreSQL, or needs durable storage at team / multi-node scale | `PostgresEventStore` |
| Runs on serverless / a read-only or ephemeral filesystem | `RedisEventStore` or `PostgresEventStore` (never SQLite) |

> **Any replica count > 1 needs a *shared* store (Redis/Postgres), not SQLite.**
> A local SQLite file is visible only to the process that opened it, so behind a
> load balancer (or during a rolling deploy, when a reconnecting client lands on
> a different pod) that pod won't have the client's events and the resume
> silently returns nothing. SQLite is for a genuine single process. See
> [deployment topologies](docs/production.md#deployment-topologies-rolling-deploys-load-balancers-serverless).

How they compare:

| | SQLite | Redis | Postgres |
|---|---|---|---|
| External service | None | Redis | PostgreSQL |
| Multi-process / multi-worker | No (single writer) | Yes | Yes |
| Durable across restarts | Yes (on disk) | Depends on Redis persistence config | Yes |
| Automatic expiry | No (call `purge_expired()`) | Yes (native key TTL) | No (call `purge_expired()`) |
| Best fit | Single node, edge, local dev | Load-balanced / ephemeral fan-out | Teams already running Postgres |

Per-backend construction, configuration, write-behind tuning, and multi-tenant
setup live in **[docs/backends.md](docs/backends.md)**; latency and throughput
characteristics in **[docs/benchmarks.md](docs/benchmarks.md)**.

## Installation

```bash
# SQLite backend (no external service needed)
pip install "mcp-persist[sqlite]"

# Redis backend
pip install "mcp-persist[redis]"

# Postgres backend
pip install "mcp-persist[postgres]"

# Multiple backends
pip install "mcp-persist[sqlite,redis,postgres]"
```

## Programmatic features at a glance

Beyond drop-in resumability, every store exposes a small set of building blocks.
Full API and examples in **[docs/api.md](docs/api.md)**.

- **`subscribe()`** — push new events to an in-process consumer as they're written (Redis pub/sub, Postgres `LISTEN`/`NOTIFY`, SQLite polling).
- **`migrate()`** — copy events between backends (e.g. SQLite → Postgres as you grow), preserving per-stream ordering.
- **`compression="gzip"`** — transparently gzip large payloads above a threshold; decompression on read is automatic and config-independent.
- **Metrics** — pass a `metrics=` collector (a `Protocol`, or the built-in `LoggingMetricsCollector`) to emit to Prometheus/Datadog/etc.; zero overhead when unused.
- **`PurgeScheduler`** — run `purge_expired()` on an interval for SQLite/Postgres (Redis expires natively).
- **`event_store_from_env()`** — pick the backend at deploy time from `MCP_PERSIST_*` env vars, no branching in code.
- **`ping()`** — backend liveness/readiness probe for health endpoints.

## Architecture & guarantees

- **Ordering** — event IDs are monotonically increasing; replay order is preserved **per-stream**.
- **Concurrency** — duplicate IDs are structurally impossible (`AUTOINCREMENT` / `IDENTITY` / `INCR`); Redis and Postgres take concurrent writes, SQLite is single-writer.
- **Durability** — SQLite uses WAL, Postgres is ACID, Redis depends on its persistence config (use AOF for strong durability).

Full treatment — including the Redis write-ceiling caveat — in
**[docs/architecture.md](docs/architecture.md)**.

## Examples

The [`examples/`](examples/) directory contains minimal, runnable MCP servers:
the `with_persistence()` one-liner, plus each backend wired manually into a real
[`StreamableHTTPSessionManager`](https://github.com/modelcontextprotocol/python-sdk):

| File | Approach | Run |
|---|---|---|
| [`fastmcp_plugin_server.py`](examples/fastmcp_plugin_server.py) | `with_persistence()` plugin (SQLite) | `python examples/fastmcp_plugin_server.py` |
| [`sqlite_server.py`](examples/sqlite_server.py) | Manual `SQLiteEventStore` | `python examples/sqlite_server.py` |
| [`redis_server.py`](examples/redis_server.py) | Manual `RedisEventStore` | `python examples/redis_server.py` |
| [`postgres_server.py`](examples/postgres_server.py) | Manual `PostgresEventStore` | `python examples/postgres_server.py` |

Each one is a self-contained MCP server you can connect to with any MCP client at
`http://localhost:8000/mcp` (the three backend servers are a note-taking app; the
plugin server is a minimal echo server). See
[`examples/README.md`](examples/README.md) for prerequisites, setup, and a client
snippet.

## Benchmarks

Measured at `--events 5000 --concurrency 500` (AMD Ryzen AI 7 350, local Redis 8 /
Postgres 18 — indicative, not authoritative):

| Backend | store throughput | replay 1,000 |
|---|---|---|
| SQLite | 23,517 ev/s | 6.51 ms |
| Redis | 7,857 ev/s | 8.79 ms |
| Postgres | 7,427 ev/s | 6.58 ms |

Full methodology, environment spec, percentiles, and analysis in
**[docs/benchmarks.md](docs/benchmarks.md)**. Run it yourself with
`uv run python benchmarks/benchmark.py --events 5000 --concurrency 500`.

## 📚 Documentation

| Guide | What's in it |
|---|---|
| [docs/backends.md](docs/backends.md) | Manual wiring, per-backend config, write-behind commits, multi-tenant isolation, `create()` lifecycle |
| [docs/cli.md](docs/cli.md) | `doctor` & `stats` full reference: sample output, `--json`, exit codes |
| [docs/api.md](docs/api.md) | `subscribe`, `migrate`, metrics, compression, `PurgeScheduler`, env config, `ping` |
| [docs/architecture.md](docs/architecture.md) | Event ordering, concurrency & write semantics, consistency & durability |
| [docs/benchmarks.md](docs/benchmarks.md) | Benchmark methodology, environment spec, full result tables |
| [docs/production.md](docs/production.md) | Deployment topologies, sizing, failure modes, TLS/credentials, checklist |
| [docs/typescript.md](docs/typescript.md) | Proxying a TypeScript (or any non-Python) MCP server |

## Deploying to production

Once a backend is wired in, see the **[production guide](docs/production.md)** for
operating it: scheduling `purge_expired()` so storage doesn't grow without bound,
treating the store as a critical dependency (failure modes), pre-creating schema
under restricted database permissions, TLS and credential handling, connection
and pool sizing across workers, and a deployment checklist.

## Development

```bash
git clone https://github.com/Ar-maan05/mcp-persist
cd mcp-persist
uv sync --all-extras --dev
uv run pytest tests/
```

The suite is 300+ async tests covering all three backends. The Redis tests use
[fakeredis](https://github.com/cunla/fakeredis-py) and the SQLite tests use
in-memory `aiosqlite`, so the default run needs no external servers. The Postgres tests require a real server and are skipped unless
`MCP_TEST_POSTGRES_URL` is set; to run them and the Redis suite against real
backends:

```bash
MCP_TEST_REDIS_URL=redis://localhost:6379/0 \
MCP_TEST_POSTGRES_URL=postgresql://postgres@localhost:5432/postgres \
uv run pytest tests/
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for more.

## License

MIT
