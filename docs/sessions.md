# Durable sessions

A persistent event store keeps the events. It does not keep the *session*.

The MCP Python SDK's `StreamableHTTPSessionManager` holds its live sessions in
`self._server_instances`, an ordinary in-process dictionary. A session id it has
never seen gets a 404. So after a restart the picture is this: the events are
still in SQLite, Redis or Postgres, the client still has its `Mcp-Session-Id` and
its `Last-Event-ID`, and it still cannot get to them, because the process it is
talking to has never heard of that session. The same thing happens when a
request lands on a second worker, which is why resumability across a load
balancer has needed sticky routing.

`durable_sessions=True` closes that gap: session ids are recorded in the same
store the events already live in, and a process that meets an id it did not
create looks it up and resumes it instead of rejecting it.

```python
from mcp.server.mcpserver import MCPServer
from mcp_persist import with_persistence

mcp = MCPServer(name="MyServer")
app = with_persistence(mcp, backend="sqlite", url="events.db", ttl=3600, durable_sessions=True)
```

Or from the environment, alongside the other `MCP_PERSIST_*` settings:

```bash
export MCP_PERSIST_DURABLE_SESSIONS=1
```

It is off by default. With it off, behaviour is byte-for-byte the upstream
manager's.

## What resuming does and does not restore

Adoption restores the session's **identity** and its **event history**. That is
what stream resumability is defined in terms of: the client reconnects quoting
the last event id it saw, and the server replays what came after. Because the
event store is shared, the replay is the same on any worker.

It also restores the **handshake**. The client's `initialize` params (protocol
version, capabilities, client info) are recorded with the session, and the
process that adopts it starts the connection already initialized from them. The
client completed `initialize` with the process that created the session and will
not send it again; without this, an adopted session answered every method but
`ping` with `-32602`. Sessions recorded by a release before this one carry no
handshake and are adopted uninitialized, as before.

It does not restore **server-side conversation state**. A transport is a live
pair of streams; it cannot be serialized and moved. A tool call that was still
running when the process died is gone, and the client finds out the way it always
does, by never receiving the result. If your tools hold state in process memory
between calls, that state does not survive, with or without this feature.

So: reconnects, restarts, rolling deploys and non-sticky load balancing are
covered. Resuming a half-finished computation is not.

## The registry

Sessions are stored next to the events, sharing the store's connection, so there
is no second pool to configure and no extra connection. On SQLite that sharing is
required rather than merely tidy, since a second handle to the same file would
contend for the write lock.

| Backend  | Where it lives                                              |
| -------- | ----------------------------------------------------------- |
| SQLite   | `mcp_sessions` table in the same database file               |
| Postgres | `mcp_sessions` table in the same database                    |
| Redis    | `{prefix}session:{id}` hashes, indexed by a `{prefix}sessions` sorted set |

Change the SQL table with `session_table_name=`. The record holds a session id,
when it was created and last seen, whether it has been terminated, and the
authorization context of the principal that created it. Nothing about the
conversation is in there: the events stay the event store's job.

Reach it directly when you need to:

```python
from mcp_persist import session_registry_for

registry = session_registry_for(store)
await registry.initialize()

for record in await registry.list_sessions():
    print(record.session_id, record.last_seen_at)
```

The registry is tenant-scoped through the store it is built from, so a
multi-tenant deployment cannot see, or adopt, another tenant's sessions.

## Credentials are still enforced

The SDK binds a session to the credential that created it and answers a mismatch
with the same 404 it uses for an unknown session, so an attacker cannot tell the
two apart. Adoption must not become a way around that, so the registry stores the
creating principal's `AuthorizationContext` (`client_id`, `issuer`, `subject`) and
compares it before resuming. The comparison is exact, and an unauthenticated
request cannot pick up an authenticated session or the reverse.

A session that has been terminated, by a client `DELETE`, an idle timeout, a
crash, or an operator, is never adopted again. Re-registering a terminated id
does not revive it.

If the registry itself is unreachable, a lookup failure is logged and the request
falls back to the upstream 404 rather than becoming a 500. A registry outage
costs you resumability, not availability.

## Operating

```bash
mcp-persist sessions list                 # live sessions, most recent first
mcp-persist sessions list --all --json    # include terminated, machine-readable
mcp-persist sessions show <session-id>
mcp-persist sessions terminate <session-id>
mcp-persist sessions purge --older-than 30d
```

`terminate` ends a session immediately and permanently: no worker will adopt it
again, which is the tool to reach for when a session needs to be cut off. It
exits non-zero if the id does not exist, so a typo is distinguishable from a
session that really was ended.

Session records are small, but they are not self-cleaning on the SQL backends
(Redis expires them with the store's ttl). Run `sessions purge --older-than` on a
schedule, matching the window you use for events. `--older-than` counts from when
the session was last seen, not when it was created.

An empty `sessions list` on a server you expect to be busy almost always means
`durable_sessions` was never turned on.

## Wiring it yourself

If you build the session manager rather than using `with_persistence`:

```python
from mcp_persist import ResumableSessionManager, session_registry_for

registry = session_registry_for(store)
await registry.initialize()

manager = ResumableSessionManager(
    app=mcp._lowlevel_server,
    event_store=store,
    registry=registry,
)
```

Pass `adopt_sessions=False` to record sessions without ever resuming one. The
registry then works purely as an inventory for `mcp-persist sessions`, and
unknown ids 404 exactly as upstream. That is a reasonable first step if you want
the visibility before you change how reconnects behave.
