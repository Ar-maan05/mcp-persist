# Records: persistence that works on every protocol version

Events and durable sessions only apply to clients that negotiate a handshake-era
protocol revision. **Records apply to every revision**, including the stateless
`2026-07-28` transport where the event store is never consulted at all. If you
run one thing from this library across a protocol migration, run this.

## The problem records solve

The MCP SDK routes each request by its `MCP-Protocol-Version` header. A version
outside the handshake-era set goes to a self-contained single-exchange handler:
no `initialize` handshake, no `Mcp-Session-Id`, one JSON-RPC request in and one
response out. That path never constructs the transport that holds your
`EventStore`, so:

- there is no stream to replay, and no event id on the wire to replay from;
- there is no session id to persist;
- a configured event store is simply never asked to store anything.

None of that is a bug in the SDK or in this library. It is the protocol getting
smaller on purpose. But it does mean a deployment can be configured for
persistence and quietly persist nothing for a modern client, which is why
`mcp-persist` now warns about it once per protocol version and offers a surface
that does keep working.

## Support matrix

| | handshake era (`2024-11-05` … `2025-11-25`) | `2026-07-28` and later |
|---|---|---|
| Durable event store | yes | no, never consulted |
| SSE replay / resumability | yes | not possible: no event id on the wire |
| Durable sessions | yes | not applicable: no session id exists |
| **Durable records** | **yes** | **yes** |

## Turning them on

```python
from mcp.server.mcpserver import MCPServer
from mcp_persist import with_persistence

mcp = MCPServer(name="MyServer")
app = with_persistence(mcp, backend="sqlite", url="events.db", record=True)
```

Recording is off by default, because it changes what an existing deployment
writes. `MCP_PERSIST_RECORD=1` turns it on from the environment.

Records land next to the events, in their own `mcp_records` table (or Redis key
prefix) with their own retention, and inherit everything the event store already
gives you: tenant binding, compression, encryption at rest, and the retention
machinery.

```python
app = with_persistence(
    mcp,
    backend="postgres",
    url="postgresql://localhost/app",
    record=True,
    record_table_name="mcp_records",
)
```

### Retention

`record_ttl` applies to **Redis only**, which is the one backend that expires
keys natively. Passing it with `backend="sqlite"` or `"postgres"` raises rather
than quietly doing nothing, because a retention setting that looks configured
and is not would be worse than no setting at all.

```python
app = with_persistence(mcp, backend="redis", url="redis://localhost:6379", record=True, record_ttl=30 * 86400)
```

On SQLite and Postgres, bound record growth explicitly:

```python
await app.state.record_store.purge(older_than=30 * 86400)
```

The live record store and its writer are published on `app.state.record_store`
and `app.state.record_flusher`.

## What is in a record

```python
Record(
    protocol_version="2026-07-28",  # which era handled it
    method="tools/call",
    kind="request",  # or "notification"
    outcome="ok",
    carrier="middleware",
    duration_ms=12.5,
    request_id="7",
    tool_name="search",
    error_code=None,
    payload=None,
    payload_truncated=False,
)
```

`outcome` is one of:

| outcome | meaning |
|---|---|
| `ok` | the handler returned normally |
| `tool_error` | a `tools/call` returned `isError`; the request itself succeeded |
| `mcp_error` | the handler raised an `MCPError`; `error_code` carries the code |
| `validation_error` | the params failed validation |
| `exception` | anything else raised |
| `cancelled` | the client disconnected and the handler's task group was cancelled |

A tool failing is not a transport failure, and the two stay distinguishable.

**There is no message or detail field, by design.** Validation messages and
exception strings quote the client's own input back, so a record carries a fixed
outcome and a numeric code and nothing else. There is nowhere for that text to
go even by accident.

## What is always recorded

`PayloadPolicy` governs **params**. It does not govern the identifiers that make
a record mean anything, which are written whatever the policy says:

`method`, `tool_name`, `protocol_version`, `request_id`, `outcome`,
`error_code`, `duration_ms`, and the timestamp.

Three of those are chosen by the client rather than by you: `method`,
`tool_name`, and the JSON-RPC `request_id`. Nothing in the protocol stops a
client putting arbitrary text in a request id, so each is clipped to 512 bytes
before it reaches the store. They are still stored, and they are covered by the
store's encryption when a keyring is configured, but they are not filtered by
the allowlist. **If your clients put sensitive values in request ids, enable
encryption at rest** ([docs/encryption.md](encryption.md)); the allowlist alone
will not protect you there.

## Capturing params: off by default, allowlist only

Tool arguments are arbitrary third-party input: credentials, tokens, personal
data, whole documents. Capture is therefore an allowlist, and captures nothing
until you name fields explicitly.

```python
from mcp_persist import PayloadPolicy

policy = PayloadPolicy(
    tool_arguments={"search": ["query"], "deploy": ["config.region"]},
    method_params={"resources/read": ["uri"]},
    max_value_bytes=4096,
    max_record_bytes=16384,
)

app = with_persistence(
    mcp, backend="sqlite", url="events.db", record=True, record_payload_policy=policy, keyring=keyring
)
```

The rules, and why each exists:

- **Top-level scalars only.** Allowing `config` when its value is an object would
  capture the whole subtree, including fields a tool author added that you have
  never seen. A bare name matches scalars; reaching inside requires the exact
  path, `config.region`.
- **Omission, not masking.** A field that is not allowed is absent rather than
  replaced with a placeholder, because the key name alone can be the sensitive
  part (`patient_ssn` says plenty even as `"***"`).
- **Truncation before storage**, so a capped value is what reaches the
  compression and encryption codecs. `payload_truncated` marks any record the
  policy shortened or dropped a field from.
- **A whole-record cap** on top of the per-value cap, so a wide object cannot add
  up to an unbounded row.
- **A keyring is required.** Turning capture on against a store with no keyring
  raises, since these are the most sensitive bytes this library ever writes. Pass
  `record_allow_plaintext=True` to accept plaintext deliberately.

This narrows what is stored to what you named. It cannot promise that a field
you allowed is free of secrets; that judgement stays with you.

## Records never slow down or break a request

Writing happens off the request path. Submitting a record is a synchronous,
non-blocking put onto a bounded queue, drained in batches by a background task
owned by the application lifespan. A slow or unreachable record backend costs a
request nothing.

The trade is explicit: **a full queue drops records.** Blocking a request to
write observability data would be the wrong answer, so the queue drops and
counts instead. Drops are never silent:

```python
app.state.record_flusher.stats()
# {'written': 1180, 'dropped': 0, 'failed': 0, 'pending': 3, 'running': True}
```

Pass `record_metrics=` to route the same counts into your monitoring:

```python
app = with_persistence(mcp, backend="redis", url=..., record=True, record_metrics=my_collector)
```

The collector receives `on_record_write`, `on_record_drop` and `on_error`. The
hooks are feature-detected, so a collector written before records existed keeps
working untouched.

Loss is accounted for in three distinct ways, and none of them is silent:

| counter | meaning |
|---|---|
| `dropped` | the queue was full, the writer was cancelled mid-write, or a shutdown drain timed out |
| `failed` | the backend raised, or accepted fewer records than were submitted |
| `written` | confirmed by the store's own returned count, not assumed from the batch size |

Tune the ceiling with `record_max_queue` (default 10000). On shutdown the writer
drains what is queued within a short budget; anything still unwritten past it is
counted as dropped rather than hanging process termination.

## The bypass warning

With an event store configured, the first request on a stateless protocol
version logs one warning naming that version, then stays quiet. It is
information, not an error: nothing is misconfigured, but the events you might
have expected are not being written.

`warn_on_bypass=False` turns it off. `mcp-persist doctor` reports the same
boundary as a `protocol support` check.

## What records are not

A record is not a replay token, and cannot resume a stream. Resumability needs
an event id on the wire, and the modern transport does not emit one. Records
tell you what happened; they do not let a client pick up where it left off.
