# Using mcp-persist with a TypeScript MCP server

Most MCP servers are written in TypeScript, and `mcp-persist` adds durable SSE
resumability to them without a line of code on the server side.

You do not import anything into your Node process. `mcp-persist`'s `EventStore`
backends are a Python API and cannot be loaded from TypeScript, but you do not
need them to be. The [`PersistenceProxy`](../README.md#resumability-without-touching-the-server-persistenceproxy)
is a standalone HTTP proxy: it sits in front of your server, intercepts the SSE
responses, stores every event under its own monotonic IDs, and replays them when
a client reconnects with `Last-Event-ID`. Because it speaks plain HTTP, it does
not care what language the upstream is written in.

## How it fits together

```
MCP client  <-->  mcp-persist-proxy  <-->  your TypeScript MCP server
                  (the event store)        (no event store needed)
```

Your clients connect to the proxy instead of the server. Nothing on the client
changes, and the upstream needs no event store of its own: the proxy is the
store.

## What you need

- A TypeScript MCP server that speaks the **Streamable HTTP** transport. The
  proxy works by intercepting SSE responses, so a stdio server will not work:
  expose your server over HTTP first.
- Python 3.10+ with the proxy installed. The proxy CLI itself pulls in no extra
  dependencies, but each backend does, so install the one you want:

  ```bash
  # pipx keeps it isolated from the rest of your system Python
  pipx install "mcp-persist[sqlite]"     # or [redis], or [postgres]
  ```

  This puts the `mcp-persist-proxy` command on your PATH.

## Option A: point the proxy at a running server

If your TypeScript server is already up (say on port 8001), point the proxy at
it and serve the proxy on the port your clients use:

```bash
mcp-persist-proxy --upstream http://localhost:8001 \
    --backend sqlite --url events.db --port 8000
```

Now point your MCP clients at `http://localhost:8000/mcp` instead of `:8001`.

## Option B: let the proxy launch the server

The proxy can also start your Node process, wait until it answers, and stop it
when the proxy exits. Put the command after `--`. The server must listen on the
port given by `--upstream-port` (default 8001), so pass that port through to it:

```bash
mcp-persist-proxy --backend redis --url redis://localhost:6379 --ttl 3600 \
    --port 8000 --upstream-port 8001 \
    -- node build/server.js --port 8001
```

The `--path` flag (default `/mcp`) sets the MCP endpoint path the proxy exposes
and forwards to.

## Choosing a backend and sizing `ttl`

- **One proxy process:** SQLite is fine. It needs no separate service.
- **More than one proxy replica:** use a shared store (Redis or Postgres). A
  client can reconnect through a different replica than the one that issued its
  `Last-Event-ID`, and a per-replica SQLite file will silently replay nothing.
  See [deployment topologies](production.md#deployment-topologies-rolling-deploys-load-balancers-serverless).
- **Set `ttl` to at least twice the upstream's session idle timeout**, so an
  event is still on hand when a slow client reconnects.

## What it covers, and what it does not

The proxy covers client disconnects against a stable upstream: a dropped
connection replays cleanly. It does **not** bridge an upstream restart. If the
TypeScript server itself restarts it begins a fresh session, and the proxy can
replay only what it already stored, not stitch the old stream onto the new
server. This is the same boundary as
[resumability scope](production.md#scope-what-resumability-does-and-does-not-cover),
one hop out.

## Running it in production

Everything in the [production guide](production.md) applies to the proxy's store
unchanged: backend choice, `ttl` sizing, scheduled
[`purge_expired()`](production.md#2-reclaiming-space-schedule-purge_expired),
schema and permissions, and observability. Two things specific to running as a
proxy:

- It is an **extra hop and a single point of failure** in front of your server.
  Run it like any other critical edge proxy, with health checks, a restart
  policy, and alerting.
- Multiple proxy replicas need a **shared store**, for the reason above.

See [Proxy mode](production.md#proxy-mode-resumability-without-modifying-the-server)
in the production guide for the full treatment.
