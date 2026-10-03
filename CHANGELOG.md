# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security
- **Sessions no longer replay each other's events.** The SDK's transport names the streams it stores after the JSON-RPC request id they belong to (or `_GET_stream`), not the session, and every session shares the event store. Two sessions that both sent a request with id `7` wrote into one stream, and resuming it with `Last-Event-ID` replayed both sessions' events. Clients number their requests from 0 or 1, so this happened in ordinary use: a client resuming a stream could be handed another session's response, under the request id it was waiting on. And anyone with a session could read other sessions' tool results by sending a request with a common id and resuming from their own event id. This affected `with_persistence()`, with or without durable sessions, and any store passed to the SDK's `StreamableHTTPSessionManager` directly, as the docs and examples showed. `with_persistence()` and `ResumableSessionManager` now give each session a view of the store that names its streams `<session_id>:<stream>` and replays a stream only to the session that owns it. **If you wire a store into the SDK's manager yourself, switch to `SessionScopedSessionManager`**, which takes the same arguments. Events stored before upgrading carry no session prefix, so a client resuming from one of them after the upgrade gets nothing replayed, as if it had expired. `PersistenceProxy` was not affected: it already scoped streams to sessions.
- **A request refused by one worker can no longer end a session for everyone.** When a worker did not hold a session and declined to adopt it (the request carried the wrong credential), it answered `404` and then recorded the session as terminated in the shared registry, treating "not held here" as "ended". Anyone who knew or guessed a live session id could send one request with any other credential to any worker that did not hold it, and the real owner could no longer resume the session after a restart or on another worker. A termination is now recorded only when the worker held the session as the request began and does not afterwards (an explicit `DELETE`).

### Fixed
- **An adopted durable session serves ordinary requests.** A process that adopted a session started a fresh connection that had never seen the client's `initialize`, and the SDK refuses every method but `ping` until it has, so after a restart or on another worker `tools/list`, `tools/call` and the rest failed with `-32602`. The `initialize` params are now recorded with the session, and the adopting process restores the handshake from them. Sessions recorded before this release have no handshake and are adopted as before.
- **A resumed durable session no longer stops a worker from serving every other session.** A worker adopting a session handled the request while still holding the lock that guards creating sessions, and a client resuming usually opens a standalone `GET` stream that stays open as long as it is connected. Until that client went away, no other session could be adopted or created on the worker: after a restart, where every client reconnects at once, all of them stalled behind the first. The lock is now released before the request is served.
- **A refused request no longer refreshes a session's last-seen time.** On the worker holding a session, a request with the wrong credential gets a `404`, but the bookkeeping afterwards still touched the session's registry record. Anyone who knew a session id could keep it looking active, and out of an age-based `purge`, for as long as they kept sending requests. Only requests the session actually served count now.
- **A session registry outage no longer fails requests.** Looking a session up and recording a new one already tolerated an unreachable registry, but updating a session's last-seen time and recording its end did not. A worker adopting a session answered `500` even though it was ready to serve it, and requests on live sessions raised after their response had gone out. Those writes are now logged and skipped, like the others.
- **Re-registering a session on Redis no longer rebinds its owner.** The SQL registries keep the principal that created a session and only refresh its last-seen time, but the Redis registry overwrote the stored owner with whatever the new call passed. Since adoption compares the requesting principal against that owner, a second `register` for a live id naming another principal handed the session to them (or, passing no owner, made it adoptable by an unauthenticated caller). The session manager only registers fresh ids, so this was reachable through direct use of the registry rather than normal traffic; Redis now keeps the original owner like the other backends.
- **Re-registering a session on Redis can no longer revive one that was just ended.** The Redis registry read the session's hash and wrote it back, so a `terminate` landing between the two was overwritten with the stale live flag, and the ended session could be adopted again. The fields fixed at creation (created time, terminated flag, owner, handshake) are now written with `HSETNX` in a single `MULTI`/`EXEC`, so there is no read to go stale.
- **Adopting a session respects the SDK's session limit.** mcp 2.2 caps the sessions a manager holds (`max_sessions`, 10000 by default) and refuses a new one past it with a `503`. A worker adopting durable sessions created them without that check, so after a restart it could end up holding any number. Adoption now counts against the limit and is refused the same way.
- **Shutting down no longer swallows errors from a session's teardown.** During shutdown the termination hook skipped recording with a `return` inside a `finally` block, which discarded whatever the transport's `terminate()` raised, including the cancellation shutdown delivers. Python 3.14 also warns about the construct (PEP 765).
- **An expired Redis session can no longer come back without an owner.** The Redis registry's `touch` checks that a session's key exists and then writes its last-seen time in a second round trip. If the key's ttl ran out in between, the write recreated it holding only that one field, which read back as a live session with no owner and no creation time: an expired session that any unauthenticated caller could adopt again. A hash without the creation time `register` always writes is no longer treated as a session.
- **A session created by a request that was then cancelled is still recorded as ended.** The session manager installed its termination hook only after the upstream handler returned, so a request cancelled or failing once the session existed left it unhooked. When that session later idled out, the registry kept listing it as live and any worker would adopt it. The bookkeeping now runs in a `finally`, shielded from the cancellation. On mcp 2.2, where the SDK discards such a session at once, it was recorded as its response started and then never marked ended at all; a session recorded during a request and gone by its end is now marked ended.
- **`RedisEventStore.create()` no longer leaks a connection on redis-py 4.x.** On redis-py older than 5.0, a client made with `from_url()` does not own its connection pool, so the `close()` that `create()` falls back to there released the connection to the pool but never closed it: one open socket per `create()`, closed only by the garbage collector, by which point its event loop could be gone (`RuntimeError: Event loop is closed`). `create()` now closes the pool explicitly. redis-py 5.0 and later were not affected.

### Added
- `SessionScopedSessionManager`, a drop-in for the SDK's `StreamableHTTPSessionManager` that keeps each session's events to itself in a shared store, and `SessionScopedEventStore`, the per-session view of a store it gives each transport. See the Security entry above.
- `mcp-persist sessions list` and the dashboard's sessions table show each session's client (`name version` from its `initialize`), and `SessionRecord.client` exposes it. The client chooses that text, so control characters are replaced and it is capped at 64 characters before it reaches a terminal. Sessions recorded without a handshake show `-`.
- `SessionRecord.handshake`, and a `handshake=` keyword on `SessionRegistry.register`. The SQL registries add a `handshake` column to an existing table on startup. A custom registry whose `register` does not take `handshake` keeps working; its sessions are adopted uninitialized.
- Python 3.14 is supported and tested. CI also runs the suite against the oldest release of every runtime dependency the package allows (mcp 2.0.0, redis 4.2.0, and so on), and checks the built wheel installs and imports on its own.

### Changed
- **Replaying a Redis stream is about twice as fast.** On a standalone Redis, replay now reads the stream index and the payloads together in a server-side script, 1000 events per call, instead of reading the index and then fetching every payload in a pipeline. A 10,000-event replay went from about 130 ms to about 65 ms in our measurements, events reach the client chunk by chunk rather than after the whole stream is read, and each call is short, so a long replay does not block the server. Redis Cluster and servers without scripting keep the pipelined reads.
- `PersistenceProxy` refuses a `Last-Event-ID` from another session before reading that session's stream, rather than after. The other stream's events were never sent, but they were read, decrypted and held in memory first. Every backend in this package can name an event's stream with one lookup, and the proxy now asks that first.
- The encryption docs no longer claim that a party who can write to the store cannot forge events. Encryption at rest protects payload confidentiality and catches an encrypted payload edited in place or moved to another stream, but a store still reads unencrypted payloads as they are (so data from before encryption stays readable), and a writer can delete or reorder events. The threat model in `docs/encryption.md` now says so.

## [2.1.1] - 2026-09-26

**A security and correctness release.** Every change here is a fix found in a full review of the package. Upgrading is recommended for everyone, and strongly for anyone running the proxy, the dashboard, batching on Postgres, multi-tenant stores or encryption at rest. One behavior needs attention during a rolling upgrade: see the encryption entry.

### Security
- **The proxy no longer replays stored history before the upstream has accepted the request.** On a `GET` with `Last-Event-ID`, the proxy read the session's events out of its store and streamed them before the upstream saw the request, so knowing a session id was enough to read that session's history, whatever credential came with it. The upstream is now asked first, and the store is read only once it has accepted. A live stream opened under one credential is no longer handed to a request carrying a different one without checking that credential with the upstream too.
- **The proxy forwards an upstream refusal instead of hiding it.** A `GET` the upstream rejected (a `401`, a `404` for an unknown session) was answered with an empty `200` event stream, so a client could not tell a bad credential from a quiet session. The upstream's status and body now reach the client unchanged.
- **The dashboard refuses requests addressed to a foreign host.** Binding to `127.0.0.1` did not stop a web page the operator had open from reaching it: the page could re-point its own hostname at the loopback address (DNS rebinding) and read every stored message, decrypted, as same-origin. Requests must now name the dashboard by `localhost`, `127.0.0.1`, `[::1]` or the address it is bound to; `--allowed-host` adds others.
- **Database passwords are no longer printed.** `redact_url` existed but only `config` used it: `doctor`, `stats`, `purge`, `dump`, `load`, `sessions` and the dashboard startup line printed the store URL with its password, and the dashboard served it from `/api/overview`. All of them now mask it, including a `password=` query parameter.
- **The zstd decompression-bomb cap now holds.** One-shot `zstandard` decompression allocates the content size a frame's header declares, ignoring the output limit, so a 32 KB `zs:` payload claiming 1 GiB allocated 1 GiB before the cap was checked. Decompression now goes through a stream reader that never produces more than the cap. The gzip path was already bounded.
- **`subscribe()` respects the tenant binding.** On SQLite and Postgres a tenant-bound store's subscription received another tenant's events whenever the two used the same stream id. Both now filter by tenant and stream, and Redis and Postgres also ignore a notification naming an event from another stream.
- **Encrypted payloads are bound to where they are stored.** AES-GCM authenticated the bytes of a payload but not its location, so a ciphertext copied into another stream's row (or another tenant's, under a shared key) decrypted cleanly and was replayed there. Payloads are now written with the `ea:` marker and their stream id, or record id, as associated data; a copy read anywhere else fails authentication and is skipped. `en:` payloads written before this release stay readable. **Rolling upgrades:** a 2.1.0 reader cannot read `ea:` payloads, so set `MCP_PERSIST_ENCRYPTION_BIND_CONTEXT=0` (or `KeyRing(..., bind_context=False)`) until every process runs 2.1.1, then remove it.
- **`mcp-persist dump -o` creates the file readable by its owner only.** A dump is decrypted plaintext and was written with the process umask.
- **`DatabaseAuditSink` validates an explicit `audit_table`.** The name was interpolated into SQL as given.

### Fixed
- **Durable sessions survive a restart again on mcp 2.2.** From mcp 2.2 the SDK terminates every session's transport as the manager shuts down, and mcp-persist recorded each of those as the session ending, so a clean restart marked every session terminated and the next process refused to adopt any of them. Terminations caused by the manager's own shutdown are no longer recorded. Adopted sessions also follow the idle timeout the way mcp 2.2 does, where the transport pushes the deadline back while requests are in flight; they used to get one fixed deadline and expire an idle period after adoption however busy they were.
- **A new durable session is recorded before the client learns its id.** It was recorded after the response carrying its `Mcp-Session-Id` had gone out, so a client that went straight to another worker could get there first and receive a 404: an intermittent failure behind any load balancer.
- **Batched Postgres writes to a schema-qualified or mixed-case table used the wrong ID sequence.** Ids were drawn from the sequence of the same-named table on the search path, so once the two counters overlapped, ordinary writes failed with a duplicate key and batched writes silently overwrote other streams' events through `ON CONFLICT`. A mixed-case name failed outright. The table's own sequence is now resolved from its fully quoted name.
- **The proxy's SSE parser is linear.** It re-scanned everything buffered so far on every network chunk, so one large event cost time quadratic in its size, all of it synchronous on the event loop: a 4 MiB tool result stalled the whole proxy, every client included, for about 40 seconds. It now scans each chunk once (about 20 ms for the same event).
- **A fork cycle no longer hangs replay.** A stream forked from its own descendant made every replay of it walk the fork chain forever, on all three backends. The walk now stops at a repeated stream, and forking a stream into itself is rejected.
- **The SQLite session registry no longer duplicates sessions.** An unbound registry stored its tenant as `NULL`, which SQLite treats as distinct in a primary key, so its upserts never matched: re-registering an id added a second row, and a terminated session was listed again as live. It now uses `''` like the Postgres registry and the record stores, and rows from earlier versions are merged on open, with a terminated copy staying terminated.
- **`BatchingEventStore` bounds its backlog.** While flushes kept failing, every write was still accepted into the retry queue without limit. Past `max_pending` (default 10000, or `flush_max_events` if larger), `store_event` now raises.
- **A Redis archive write can no longer reissue an event id.** `_store_event_raw` raised the id counter with a separate read and write, so live writes landing between the two could have their ids handed out again. It is now one atomic server-side step.
- **The proxy forwards paths with their percent-encoding intact.** Non-MCP paths were rebuilt from the decoded path, so an encoded `%3F` became the start of a query string.
- **The dashboard's event view is cheaper and reports truncation correctly.** It exported the whole selected stream into memory on every two-second poll, and flagged a stream holding exactly `limit` events as truncated. It now keeps only the newest `limit`, and the page re-reads a stream only when it has changed.
- **`RecordFlusher` is now importable from the package root.** 2.1.0 listed it as new public API, but it was never added to `mcp_persist/__init__.py`, so `from mcp_persist import RecordFlusher` raised `ImportError`. It is the background writer the middleware builds on, so it is the name an integrator wiring records by hand reaches for. A test now checks that every name in `__all__` resolves and that the whole records API is exported.

### Added
- `mcp-persist dashboard --allowed-host NAME` (repeatable), and `create_dashboard(..., allowed_hosts=)`.
- `BatchingEventStore(..., max_pending=)`.
- `KeyRing(..., bind_context=)` and `MCP_PERSIST_ENCRYPTION_BIND_CONTEXT`.

### Removed
- `mcp_persist.recorder.drain_records`, an undocumented helper that nothing used.

## [2.1.0] - 2026-08-14

**Persistence that works on every protocol version.** Everything this library did up to here applied only to clients negotiating a handshake-era protocol revision. From `2026-07-28` the SDK routes each request to a stateless single-exchange handler with no `initialize` handshake, no `Mcp-Session-Id`, and one request in and one response out, and that path never reaches the event store at all. A deployment configured for persistence therefore persisted nothing for a modern client, with no error and no warning. 2.1 says so out loud, and adds a surface that keeps working across the boundary.

### Added
- **Records (`record=True`).** A durable note of one handled message: the method, the protocol version that handled it, the outcome, the duration, the tool name, and the JSON-RPC request id. Records are written on **every** protocol revision, including the stateless transport where the event store is bypassed, so one store keeps reporting what happened while a deployment migrates across revisions. Off by default, since recording changes what an existing deployment writes; also readable from `MCP_PERSIST_RECORD`. See `docs/records.md`.
  - **A separate surface, not reinterpreted events.** Records live in their own `mcp_records` table (or Redis key prefix) with their own retention, because records usually outlive the events they describe. `record_ttl` applies to Redis only, the one backend that expires keys natively, and passing it with a SQL backend raises rather than silently doing nothing; bound SQL growth with `purge(older_than=...)`. They share the event store's connection, tenant binding, compression, encryption at rest, and retention machinery, so nothing new needs configuring. Conflating them with replay events would have broken both the resumability contract and the `dump`/`load` format.
  - **A tool failure is not a transport failure.** The outcome is one of `ok`, `tool_error` (a `tools/call` that returned `isError`), `mcp_error`, `validation_error`, `exception`, or `cancelled`, the last being a client disconnect, which on the modern transport cancels the handler's task group. The classification mirrors the SDK's own OpenTelemetry middleware so the two agree.
  - **There is no message or detail field, deliberately.** Validation messages and exception strings quote the client's own input back, so a record carries a fixed outcome and an optional numeric error code and nothing else. The text has nowhere to go even by accident. The identifiers a record must carry to mean anything (`method`, `tool_name`, `request_id`) are outside the payload allowlist by nature, and three of them are client-chosen, so each is clipped to 512 bytes; enable encryption at rest if your clients put sensitive values in a request id.
  - New public API: `Record`, `RecordStore`, `record_store_for`, `SQLiteRecordStore`, `RedisRecordStore`, `PostgresRecordStore`, `PayloadPolicy`, `require_payload_encryption`, and `RecordFlusher`.
- **`PayloadPolicy`: allowlist-only param capture, off by default.** Tool arguments are arbitrary third-party input: credentials, tokens, personal data. Nothing is captured until a field is named explicitly, per tool or per method, because a denylist of sensitive-looking key names cannot work against schemas written by people you have never met. A bare name matches **top-level scalars only**, so allowing `config` never sweeps up a nested `config.secret_key`; reaching inside requires the exact path. A field that is not allowed is **omitted rather than masked**, since the key name alone can be the sensitive part. Values are truncated before they reach the compression and encryption codecs, a whole-record cap applies on top, and `payload_truncated` marks any record the policy shortened. Turning capture on against a store with no keyring raises rather than silently writing plaintext; `record_allow_plaintext=True` accepts that deliberately.
- **Records never slow down or break a request.** Submitting is a synchronous non-blocking put onto a bounded queue drained by a background writer owned by the application lifespan, so a slow or unreachable record backend costs a request nothing, and a failing backend does not end recording for the life of the process. The trade is explicit: a full queue drops records rather than blocking a request. Drops are never silent: they are reported through `app.state.record_flusher.stats()` and, when a `record_metrics=` collector is supplied, through the `on_record_write` / `on_record_drop` / `on_error` hooks, which are feature-detected so a collector written before records existed keeps working untouched. Loss is accounted three ways that cannot be confused: a full queue, a writer cancelled mid-write, and a drain that exceeded the shutdown budget all count as `dropped`; a backend that raises or accepts fewer records than were submitted counts as `failed`; and `written` comes from the store's own returned count rather than the batch length, so a partial write is never reported as a complete one. Size the ceiling with `record_max_queue` (default 10000). On shutdown the writer drains within a short budget, and anything unwritten past it is counted as dropped rather than hanging process termination.
- **The bypass is announced.** With an event store configured, the first request on a stateless protocol version logs one warning naming that version and then stays quiet. Nothing is misconfigured when it fires; it exists so an operator finds out from a log line instead of from an empty database. `warn_on_bypass=False` turns it off.
- **`mcp-persist doctor` reports protocol support.** A new `protocol support` check names which revisions the event store and durable sessions actually apply to, and warns when records are off, since that is the configuration in which modern clients are persisted nowhere.
- **`docs/records.md`** and a support matrix in the README, stating plainly what applies per era.

### Notes
- **Modern SSE replay is not possible and is not claimed anywhere.** The stateless transport emits SSE frames with no `id:` field and has no `Last-Event-ID` handling, so there is no resumption cursor on the wire. Records tell you what happened; they do not let a client pick up where it left off. Resumability and durable sessions remain handshake-era features and are unchanged by this release.
- Every use of the SDK's middleware surface is confined to a single `mcp_persist.middleware` module, because `MCPServer.middleware` is documented upstream as provisional: the imports, the context interpretation, the protocol-era constants, and the registration call itself (`install()`), which the plugin goes through rather than touching `mcp.middleware` directly. If that surface moves, one file moves with it. The record model, the stores, the writer, the dashboard and the CLI stay SDK-free and would keep working through the proxy carrier.

## [2.0.0] - 2026-08-01

**Sessions survive now, not just events.** Every release up to here made the *events* durable and left the *session* in a process-local dictionary, which meant a restart still cost you the conversation: the events were on disk, and the client could not reach them. 2.0 records sessions in the same store and resumes them from any process.

This release also re-versions the mcp 2.0 requirement. 1.12.3 raised the floor to `mcp>=2.0.0` and added `httpx2`, a breaking dependency change shipped under a patch number, so anyone on mcp 1.x taking what looked like a routine upgrade had the SDK swapped under them. **1.12.3 is yanked**; install 2.0.0 for the mcp 2.x line, or pin `mcp-persist==1.12.2` to stay on mcp 1.x.

### Added
- **Durable sessions (`durable_sessions=True`).** The SDK's `StreamableHTTPSessionManager` keeps its live sessions in `self._server_instances`, an in-process dict, and answers an id it has never seen with a 404. A durable event store does not change that, so until now a restart left the client holding a session id and a `Last-Event-ID` that no longer resolved to anything, and a request landing on a worker that did not create the session hit the same wall (which is why resumability behind a load balancer needed sticky routing). Session ids are now recorded alongside the events, and a process that meets an id it did not create looks it up and resumes it. Restarts, rolling deploys and non-sticky balancing keep working. Off by default, so behaviour is unchanged unless you ask for it; also readable from `MCP_PERSIST_DURABLE_SESSIONS`. See `docs/sessions.md`.
  - **What it restores is the session's identity and its event history**, which is what stream resumability is defined in terms of: the client reconnects quoting its last event id and the server replays what followed, out of the shared store, on whichever worker answered. It does **not** restore server-side conversation state. A transport is a live pair of streams and cannot be serialized, so a tool call still running when the process died is gone, and the client learns that the way it always does, by never receiving the result.
  - **The credential binding is preserved.** The SDK ties a session to the principal that created it and hides a mismatch behind the same 404 it uses for an unknown session. Adoption is not a way around that: the registry stores the creating principal's `AuthorizationContext` (`client_id`, `issuer`, `subject`) and compares it exactly before resuming, so an unauthenticated request cannot pick up an authenticated session or the reverse. A session terminated by a client `DELETE`, an idle timeout, a crash, or an operator is never resumed again, and re-registering a terminated id does not revive it.
  - **A registry outage costs resumability, not availability.** A failed lookup is logged and falls back to the upstream 404 rather than turning into a 500.
- **`SessionRegistry`, with SQLite, Redis and Postgres implementations.** A registry shares the connection of the event store it is built from, so there is no second pool to configure and no extra connection; on SQLite that sharing is required rather than tidy, since a second handle to the same file would contend for the write lock. Sessions live in a `mcp_sessions` table (`session_table_name=` to change it) or, on Redis, in `{prefix}session:{id}` hashes indexed by a sorted set so listing by recency and purging by age are single commands. The registry inherits the store's tenant binding, so one tenant can neither see nor adopt another's sessions. New public API: `SessionRegistry`, `SessionRecord`, `session_registry_for`, `SQLiteSessionRegistry`, `RedisSessionRegistry`, `PostgresSessionRegistry`, and `ResumableSessionManager` for callers who wire the session manager themselves (`adopt_sessions=False` records sessions without ever resuming one, if you want the inventory before you change how reconnects behave).
- **`mcp-persist sessions`.** `list` (most recently seen first, `--all` for terminated, `--limit`, `--json`), `show`, `terminate`, and `purge --older-than`. `terminate` ends a session permanently, which is the tool to reach for when one needs cutting off; it and `show` exit non-zero on an unknown id, so a typo is distinguishable from a session that really was ended. `purge` requires an explicit window and counts from when the session was last seen. An empty `list` says that `durable_sessions` may never have been enabled rather than printing nothing. See `docs/cli.md`.
- **`mcp-persist dashboard`: a local, read-only web view of the store.** `stats` answers "how many events" and `dump` answers "what is in this one stream"; neither helps much with the vaguer question you actually have when something looks wrong, which is whether anything is arriving and whether it looks right. One self-contained page, polling every two seconds: totals and a health dot, streams with event counts and id ranges, the events of a stream you click (newest first, labelled by JSON-RPC method or result/error, click one to expand the raw message), and the durable sessions with their state. Reads run through the same store the server uses, so payloads are shown decompressed and decrypted, and the store is opened once for the life of the process rather than per request.
  - **No external assets.** The CSS and JS are inline, so it works offline, air-gapped, and behind a corporate proxy, and there is no CDN to trust. No new dependency either: Starlette and uvicorn already arrive with `mcp`.
  - **No authentication, so it will not expose itself.** It binds `127.0.0.1` and refuses a non-loopback address unless you pass `--unsafe-bind`, treating a hostname it cannot classify as exposed. `--redact-payloads` drops message bodies server-side rather than hiding them in the page, keeping counts, event ids and method names, for a shared screen or events carrying data you would rather not render. See `docs/cli.md`.
- **`env_flag()`** for reading boolean `MCP_PERSIST_*` settings. It rejects a value that means neither true nor false instead of quietly reading `MCP_PERSIST_DURABLE_SESSIONS=ture` as off, which would disable the feature it was meant to turn on.

### Changed
- **Requires the MCP Python SDK 2.0 or newer (`mcp>=2.0.0`).** The SDK's 2.0 release, published 2026-07-28, renamed `FastMCP` to `MCPServer` and moved it from `mcp.server.fastmcp` to `mcp.server.mcpserver`, moved the wire types out of `mcp.types` into the new `mcp_types` package (where `JSONRPCMessage` is now a plain union of the four JSON-RPC models rather than a `RootModel` wrapper), renamed the low-level server attribute from `_mcp_server` to `_lowlevel_server`, renamed `streamablehttp_client` to `streamable_http_client` (now yielding two streams instead of three), and replaced `httpx` with `httpx2`. Carrying both SDK majors would mean import shims through every one of those, so this release targets 2.x only. **Stay on mcp-persist 1.12.2 if you are still on mcp 1.x**; it is feature-identical apart from the fixes below.
  - **Stored events are not affected.** The `EventStore` interface, `EventId`, `StreamId`, `EventMessage`, and `StreamableHTTPSessionManager` are unchanged in the SDK, and all four JSON-RPC variants serialize byte-identically under both majors (a `RootModel` always serialized as its root). An existing SQLite, Redis, or Postgres store keeps replaying correctly across the upgrade, with no migration and no dump/load round trip.
  - `httpx2` is now a direct dependency rather than something relied on transitively via `mcp`. That transitive assumption is exactly what broke: the SDK dropped `httpx`, and every direct import of it stopped resolving.
  - `mcp_persist.fastmcp` keeps its module path and `with_persistence()` keeps its signature, so the import you already use is unchanged; only the type of the server you hand it is now `MCPServer`.
  - **Progress notifications now arrive on the POST response stream, not the standalone GET stream.** This is an SDK behavior change, not a change here, but it matters if you wrote a client against the old routing: under 1.x a `ctx.report_progress()` was delivered on the server-to-client GET channel. Resumability is unaffected, because the server assigns an event id to every event on every stream and a reconnect quotes the last id it saw; `examples/resume_demo.py` was restructured to read progress from the tool call's own stream and now resumes with zero loss under 2.x.

## [1.12.3] - 2026-07-31 [YANKED]

Yanked: it shipped the `mcp>=2.0.0` requirement above as a patch release. Its fixes and additions are all present in 2.0.0, unchanged.

### Fixed
- **`migrate` now opens both stores with the deployment's real configuration.** 1.12.2 routed `stats`, `purge`, `dump`, and `load` through the shared store factory; `migrate` was left behind because it takes its own `--from-`/`--to-` flags, so it opened each side with nothing but a backend and a URL. Three things went wrong as a result, all of them silently: reading an encrypted source without its keyring skipped every event and still exited 0 (`migrated 0 event(s)`, no failures, no data at the destination); reading a multi-tenant source unscoped merged every tenant into one unbound destination; and a source using a non-default table name or Redis key prefix was read from the default location, so `migrate` found nothing and reported an empty store. Every store setting now has a per-side flag (`--from-ttl`, `--from-table`, `--from-key-prefix`, `--from-max-stream-length`, `--from-tenant-id`, `--from-compression`, and the `--to-` equivalents), each falling back to its `MCP_PERSIST_*` variable, so the common case (migrating the deployment the shell is already configured for) is correct with no extra flags. Encryption keys remain environment-only and one keyring serves both sides, which is also what makes a re-keying migration work: hold the old and new keys in `MCP_PERSIST_ENCRYPTION_KEYS` and select the new one with `MCP_PERSIST_ENCRYPTION_KEY_ID` to read with the old and write with the new.
- **A migration that could not read the source no longer reports success.** The stores skip an event they can read from storage but cannot decode (no key, wrong key, or genuine corruption) instead of raising, which meant the loss was invisible above the log. `MigrationResult` gained `skipped_events`, `migrate()` counts them across the run and logs a warning, and the CLI prints an explicit error naming the count and exits non-zero (`skipped_events` under `--json`). A migration that copies nothing because it cannot read the source is now distinguishable from a migration of an empty store.
- **`purge --dry-run --json` emits JSON.** It printed the prose line instead, the one flag combination in the CLI that ignored `--json`. The document is `{"purged": N, "dry_run": true}`, matching the real purge.
- **Changelog release headings link again.** Every version from 1.9.0 through 1.12.2 was written as a `[x.y.z]` reference link with no matching definition at the foot of the file, so seven headings rendered as broken references.

### Added
- **`mcp-persist config`.** Prints the store settings resolved from `MCP_PERSIST_*` and any flags: backend, url, ttl, table name, key prefix, max stream length, tenant, compression, and encryption. It opens no connection, so it answers "which store am I actually pointed at?" even when that store is down, which is the question behind most of the failures above. Nothing secret is printed: a password inside the URL is masked, and encryption is reported as the active key id plus how many keys the ring can decrypt with (key ids already travel in the payload marker), never the keys themselves. `--json` for scripts. See `docs/cli.md`.
- **`KeyRing.active_key_id` and `KeyRing.key_ids`.** The non-secret parts of a keyring are now readable without reaching into private attributes, which is what `config` reports.

## [1.12.2] - 2026-07-27

### Fixed
- **Administrative commands now open the deployment's actual configured store.** `mcp-persist` forwards tenant binding, compression, and the environment keyring to every backend for `stats`, `purge`, `dump`, and `load`. An encrypted `dump` no longer skips every payload, `load` no longer writes plaintext into an encrypted deployment, and SQLite/Postgres commands stay scoped to `MCP_PERSIST_TENANT_ID` instead of reading every tenant. The non-secret `--tenant-id`, `--key-prefix`, `--max-stream-length`, and `--compression` flags can override their environment counterparts; encryption keys remain environment-only so they never appear in process arguments.
- **Batched stores retain the optional operational API.** `BatchingEventStore` now delegates health, ping, stream enumeration/export, subscriptions, expiry maintenance, archival, and tenant-retention helpers to its inner Redis or Postgres store. Every read and maintenance operation flushes first, so environment-enabled batching cannot hide pending writes from `dump`, `migrate`, `PurgeScheduler`, or `ArchiveScheduler`. `health()` reports `backend="batching"`, the inner backend, and the current pending-write count.

### Changed
- **One shared store factory now powers environment configuration, the admin CLI, `with_persistence()`, and `PersistenceProxy.create()`.** The explicit FastMCP and proxy paths now accept `tenant_id`, `compression`, `compress_min_bytes`, `keyring`, `batch_max_events`, and `batch_max_latency_ms`, matching the production configuration surface and preventing option drift between integrations.

## [1.12.1] - 2026-07-21

### Fixed
- **Batched writes are faster and survive transient partial flush failures.** Redis flushes now use one pipeline execution and Postgres flushes use one `executemany` call instead of one round trip per event. `BatchingEventStore` also retains the failed write and untouched tail, preserves their event-ID order ahead of concurrently accepted writes, and retries background flush failures after the configured latency window. Physical flushes are serialized, writes after `aclose()` fail clearly, and ID-block consumption no longer shifts a list on every event.
- **Dump imports validate every event before writing.** A malformed event late in an `import_stream()` document can no longer leave the destination with a partially restored stream. The complete dump is parsed and validated before the first store mutation.
- **Strict retention auditing now actually halts on failure.** An audit-sink exception with `strict_audit=True` now terminates the scheduler task and is propagated by `aclose()` or context exit, matching the documented contract. Previously the outer retry handler swallowed the re-raised exception. `strict_audit=False` continues to log and retry as before.

## [1.12.0] - 2026-07-08

### Added
- **Single-stream dump and load.** `mcp-persist dump <stream>` exports one stream's events to a portable, versioned JSON document (`{"format": "mcp-persist-dump", "version": 1, ...}`) on stdout or a file (`-o`); `mcp-persist load` reads that document (from a path or stdin) back into any configured store, with `--stream-id` to restore under a different name. The document is decompressed and decrypted plaintext regardless of how the source persists payloads, priming events (no message) round-trip as `{"message": null}`, and `load` validates the envelope before writing a single event, failing closed on an unrecognized format or version. The intended use is bug reports and test fixtures: capture a failing session and replay it into a fresh store. Restoring re-stores each event with `store_event`, so the destination assigns fresh IDs (content and ordering are reproduced, not the original resumability tokens), matching `migrate()` semantics. Also available programmatically as `export_stream()` / `import_stream()`. See `docs/cli.md` and `docs/api.md`.
- **`await store.health()`.** SQLite, Redis, and Postgres stores gained a structured health probe returning a `HealthReport` (`healthy`, `backend`, `latency_ms`, and a backend-specific `detail` map) with an `as_dict()` for a `/healthz` body. It reuses the store's own `ping()` for the round trip, so `latency_ms` reflects the cost every real operation pays, and it never raises: an unreachable backend comes back as `healthy=False` with the error in `detail['error']` so a health endpoint can return 503 rather than 500. `detail` carries `size_bytes` for SQLite (logical database size via `page_count * page_size`, no filesystem access), `used_memory_bytes` for Redis, and `pool_size` / `pool_idle` for Postgres. Complements `mcp-persist doctor` (one-shot pass/fail) and `MetricsCollector` (continuous per-operation). See `docs/api.md`.
- **`DEBUG_PERSIST=1` developer mode.** Set the environment variable to a truthy value (`1`, `true`, `yes`, `on`) and every store built afterward narrates its work to stderr: `SAVE`/`LOAD` lines (with timings) from a `LoggingMetricsCollector` installed as the default collector, plus `FLUSH` (batching) and `PURGE` lines. It is a zero-config alternative to wiring a `MetricsCollector` by hand and is honored as early as package import, so lines still appear for a store constructed with a custom collector. When the flag is unset the default stays `NoOpMetricsCollector` (which the stores special-case to skip timing entirely), so there is no runtime cost. The stderr handler is only attached when the application has not already configured logging, so it never fights an existing setup.
- **`purge --older-than`.** `mcp-persist purge --older-than 30d` (also `12h`, `45m`, `3600s`, `2w`, or a bare number of seconds) deletes events by an explicit age instead of the configured ttl, and works even when no ttl is set, useful for a one-off cleanup or a store that keeps events indefinitely by default. `--dry-run` counts by the same cutoff without deleting. Backed by a new `older_than=` parameter on `purge_expired()` and `count_expired()` for the SQLite and Postgres stores (Redis expires keys natively and rejects the flag). The efficient bulk and batched `DELETE` paths are shared with the ttl-based purge. See `docs/cli.md`.

## [1.11.1] - 2026-07-03

### Added
- **`doctor` now checks compression and encryption config.** The diagnostic grew two checks so it covers the features added in 1.9 through 1.11, not just the 1.8 surface. The `compression` check runs the same `validate_compression` guard the stores run at construction, so `MCP_PERSIST_COMPRESSION=zstd` without the `zstd` extra (or an unknown codec) is reported up front with the pip hint instead of failing at the first write. The `encryption` check parses `MCP_PERSIST_ENCRYPTION_*` via `keyring_from_env` (surfacing a malformed key set as a `fail`) and, because a `KeyRing` builds without the `cryptography` package (AES-GCM is imported lazily), also flags a keyring configured while the `crypto` extra is not installed, another failure that otherwise stays silent until a write. Both are `pass` when unconfigured, since compression and encryption are opt-in. The checks appear in both the checklist and `--json` output.
- **`--version` on both CLIs.** `mcp-persist --version` and `mcp-persist-proxy --version` print the installed version and exit.

## [1.11.0] - 2026-06-30

### Added
- **Encryption at rest (AES-256-GCM).** Pass a `KeyRing` as `keyring=` to any backend (or configure it from `MCP_PERSIST_ENCRYPTION_KEY` / `MCP_PERSIST_ENCRYPTION_KEYS` plus `MCP_PERSIST_ENCRYPTION_KEY_ID`) to encrypt event payloads before they reach the store; decryption on read is automatic. Like compression, the stored form is marker-prefixed (`en:<key_id>:<base64(nonce+ciphertext)>`), so a reader recognizes ciphertext without configuration, plaintext rows written before encryption was enabled stay readable, and writers holding different keys coexist for zero-downtime key rotation. Encryption composes with compression as the outer layer (compress then encrypt on write; decrypt then decompress on read) and adds no new decompression-bomb surface. AES-GCM is authenticated, so a tampered payload fails to decrypt rather than decrypting to silently wrong bytes; the codec fails closed (a missing or wrong key raises rather than returning ciphertext), and on replay an undecryptable event is skipped with a logged warning rather than leaking ciphertext or aborting the resume. New public API: `KeyRing`, `generate_key`, `keyring_from_env`. Requires the new `crypto` extra (`pip install "mcp-persist[crypto]"`). See `docs/encryption.md`.
- **Single-round-trip Redis writes.** On a standalone (non-cluster) Redis that supports server-side scripting, `RedisEventStore.store_event` now runs as one `EVALSHA` (counter increment plus the event hash, stream-index entry, optional length trim, and optional ttls in a single server-side step) instead of an `INCR` followed by a pipelined write, halving the per-event round-trips. The store probes for script support on its first write and caches the result; it falls back to the original `INCR` plus pipeline path automatically on Redis Cluster (where the keys span hash slots) or any server without scripting, so behavior, event ids, and durability semantics are identical either way.

### Fixed
- **`event_store_from_env` now forwards `compression`, `compress_min_bytes`, and (new) `keyring` to every backend, plus `tenant_id` to Redis.** Previously each backend's `create()` did not accept these, so env-configured values fell through into the underlying connection call: on SQLite that raised a `TypeError` for `compression`, and on Redis `tenant_id` and `compression` were silently dropped (an env-configured multi-tenant or compressed Redis store was neither). The `create()` classmethods now accept these explicitly and apply them, matching the direct constructors.

## [1.10.0] - 2026-06-23

### Added
- **Event stream forking.** Support branching an existing stream (`stream_id`) at any point (a specific `fork_event_id`) and replaying from that branch with different inputs or a different model, preserving the original branch intact. This turns the linear event log history into a tree for systematic A/B evaluation. The SQLite, Redis, and Postgres stores implement `fork_stream` and update event replay (`replay_events_after` / `_iter_stream_events`) to traverse segment boundaries dynamically. The wrappers `BatchingEventStore` and `ChainedEventStore` propagate the optional `stream_id` parameter and delegate the fork registration to their inner store.
- **Per-team retention policies with audit logging.** Added support for defining per-tenant retention windows via `RetentionPolicy` and periodically purging expired events via a background `RetentionScheduler`. Deletions are captured and written to a pluggable `AuditSink`, which ships with three implementations: `NoOpAuditSink` (discards entries), `LoggingAuditSink` (logs entries as JSON), and `DatabaseAuditSink` (writes to an append-only table). Features a `strict_audit` mode that re-raises sink errors to prevent silent compliance failures. The SQLite and Postgres stores implement `purge_tenant` and `distinct_tenants` (Redis is rejected by design). Policies can be configured from environment variables `MCP_PERSIST_RETENTION_WINDOWS` and `MCP_PERSIST_RETENTION_DEFAULT` using the `retention_policy_from_env()` helper. See `docs/retention-policies.md`.

## [1.9.0] - 2026-06-21

### Added
- **Tiered storage (archive instead of delete).** New `ArchiveScheduler` moves events past their ttl out of a hot store into a cold store on an interval (read expired batch, ID-preserving upsert into cold, then delete from hot), draining the whole expired backlog each cycle so a busy store keeps up. New `ChainedEventStore` writes to the hot tier and falls back to cold on a replay miss, continuing from hot afterward in monotonic order, so a client resumes seamlessly across the tiers; cold stores preserve the original `event_id` so resumability tokens stay valid. The lower-level `archive_expired_batch()` and `count_expired()` helpers, plus the `StoredEvent` record, are exported for custom loops. See `docs/tiered-storage.md`.
- **Multi-tenancy.** Every backend accepts a `tenant_id` (or `MCP_PERSIST_TENANT_ID`) that isolates a store's events from other tenants sharing the same backend. Redis folds the tenant into its key prefix; SQLite and Postgres add a `tenant_id` column plus a `(tenant_id, stream_id, event_id)` index and scope every read and write (`store_event`, `replay_events_after`, `list_streams`, `purge_expired`, `count_expired`, `select_expired`) to the bound tenant. An unbound store (`tenant_id=None`, the default) is unscoped and sees every tenant, so existing single-tenant deployments are unchanged; a table from an older version is migrated in place on first open. See `docs/multi-tenancy.md`.
- **Batched writes for high-throughput sessions.** New `BatchingEventStore` wraps a Redis or Postgres store and buffers writes, flushing on a size threshold (`flush_max_events`, default 64) or a latency ceiling (`flush_max_latency_ms`, default 50), whichever comes first. It still returns each `EventId` synchronously by pre-allocating ID blocks from the inner store (Redis `INCRBY`, a Postgres sequence batch), so only durability is deferred; `replay_events_after()` flushes pending writes first so a reconnecting client never misses a buffered event. Configurable from the environment with `MCP_PERSIST_BATCH_MAX_EVENTS` / `MCP_PERSIST_BATCH_MAX_LATENCY_MS`. SQLite is intentionally rejected (it already batches the dominant fsync via write-behind `commit_interval` / `commit_max_pending`).
- **`mcp-persist purge` and `mcp-persist migrate` subcommands.** `purge` forces an immediate `purge_expired()` (with `--batch-size` and a `--dry-run` that only counts via `count_expired()`); `migrate` copies every stream between two backends with per-stream progress, `--json`, and a non-zero exit on any failed stream. Both resolve config exactly like `doctor`/`stats`. See `docs/cli.md`.
- **zstd compression.** `compression="zstd"` (via the new `zstd` extra) joins `"gzip"` as a payload codec, with a better ratio and speed for JSON-RPC payloads. Compressed payloads are marker-prefixed (`zs:` vs `gz:`) and decompression is keyed off the marker, so a zstd writer and a gzip writer coexist with no migration, exactly like the existing gzip rollout story. The decompression-bomb cap (100 MiB) is enforced on the zstd read path via `decompress(max_output_size=...)`.
- **OpenTelemetry export.** New `OTelMetricsCollector` (via the `otel` extra) implements the `MetricsCollector` interface on OpenTelemetry instruments, recording store/replay duration histograms and error/proxy-replay counters tagged with `backend` (and `tenant_id` when set), so the persistence layer's metrics correlate with the rest of a production stack's tracing. The recording is in-process and non-blocking, matching the synchronous collector contract.

### Changed
- `event_store_from_env()` now reads `MCP_PERSIST_TENANT_ID`, `MCP_PERSIST_COMPRESSION`, and the `MCP_PERSIST_BATCH_*` variables, and wraps the store in a `BatchingEventStore` when batching is configured (rejected for the sqlite backend). All new variables are optional, so existing configurations are unaffected.

## [1.8.4] - 2026-06-13

### Added
- **`mcp-persist-proxy --cors`**: CORS support for browser-based MCP clients. A web UI talks to the proxy through `fetch`, which requires CORS; without it the browser blocks the response because the proxy synthesizes its own headers for the SSE streams it serves (the initialize POST and the standalone GET) and never sent `Access-Control-Allow-Origin`, so the client failed with "Failed to fetch" even though the upstream had CORS of its own. With `--cors` the proxy answers the preflight (`OPTIONS`) itself (so it works even when the upstream sends no CORS headers), stamps `Access-Control-Allow-Origin` on every response it sends (SSE, JSON, passthrough, and 413), and exposes `mcp-session-id` so the client's JavaScript can read the session id. `--cors` allows any origin (`*`) by default; pass an explicit origin (`--cors https://app.example`) to restrict it, in which case any `Access-Control-*` the upstream set is dropped so the browser never sees two `Access-Control-Allow-Origin` values. The equivalent `cors=` keyword is available on `PersistenceProxy` and `PersistenceProxy.create`. CORS handling is off by default, so existing deployments are unaffected.

## [1.8.3] - 2026-06-13

### Changed
- **README restructured and slimmed (851 → ~300 lines).** The README is now a quick tour (pitch, `with_persistence()` quickstart, proxy, backend-selection tables, programmatic-feature overview, and a headline benchmark), with the full reference moved into `docs/` and surfaced through a new Documentation index. No documentation was removed; everything was relocated and cross-linked.

### Added
- **`mcp-persist-proxy --check`**: a pre-flight that probes the upstream and exits without starting the proxy. It reports a two-level pass/fail checklist (reachable, then a minimal MCP `initialize` POST confirming the endpoint speaks Streamable HTTP), catching a down upstream or a wrong `--path` before clients ever connect. A wrong path (HTTP 404/405) fails with a hint; a reachable but non-MCP response warns without failing. It requires `--upstream` (mode 1) and opens no event store, so it never touches Redis or Postgres. Exits non-zero on a failed check.
- **`on_proxy_replay` metrics hook**: `PersistenceProxy` now accepts a `metrics=` collector and fires an optional `on_proxy_replay(stream_id, session_id, events_replayed, blocked, duration_ms)` hook on every reconnect-triggered replay, across both the cold-store path and the live-buffer gap path. Unlike the store-level `on_replay` (which counts what the query returned), this reports what was delivered to the client after the cross-session ownership gate, and sets `blocked=True` for a `Last-Event-ID` that resolved to another session's stream (a signal for clients enumerating event IDs). The hook is feature-detected, so existing three-method collectors are unaffected; `NoOpMetricsCollector` and `LoggingMetricsCollector` both implement it.
- **`docs/backends.md`**: manual wiring, per-backend configuration, write-behind commits, multi-tenant isolation, and the `create()` connection lifecycle (moved out of the README).
- **`docs/cli.md`**: full `doctor` and `stats` reference, with sample output, `--json`, and exit-code semantics.
- **`docs/api.md`**: programmatic feature reference for `subscribe`, `migrate`, metrics, compression, `PurgeScheduler`, `event_store_from_env`, and `ping`.
- **`docs/architecture.md`**: event ordering, concurrency & write semantics, and consistency & durability guarantees.
- **`docs/benchmarks.md`**: benchmark methodology, environment spec, and full result tables.

## [1.8.2] - 2026-06-12

### Added
- **New `mcp-persist` admin CLI with a `doctor` subcommand**, a pass/fail diagnostic. It checks the things that usually explain a broken or silently growing store: the Python runtime against the supported floor, whether the chosen backend's driver extra is installed (with the exact `pip install 'mcp-persist[...]'` hint when it is not), live connectivity via the store's existing `ping()` (reporting the backend version on success), and retention config that lets events accumulate without bound (an unset `ttl` on Redis, `max_stream_length` set without a `ttl`, or any non-expiring SQLite/Postgres store whose `purge_expired()` would be a no-op). Config is resolved from `--backend`/`--url`/`--ttl`/`--table` or the `MCP_PERSIST_*` env vars, identical to the proxy and `event_store_from_env`. The runtime, driver, and retention checks run from resolved config even when the backend is down (which is when you reach for the doctor); an unreachable store is reported as a failed `connectivity` check, not a crash. Output is a checklist by default or `--json` for CI/readiness gates; the command exits non-zero only on a failed check, so warnings surface without breaking an exit-code gate. The existing `mcp-persist-proxy` entry point is unchanged.
- **`mcp-persist stats` subcommand**: reports the event count and event ID range (`min`/`max`) per stream, store-wide totals, the latest assigned event ID, and a latency probe timed against the backend's native `PING` / `SELECT 1`. It reads the store directly with a single cheap pass (a pipelined `ZCARD` + `ZRANGE` on Redis; one `GROUP BY stream_id` plus `MAX(event_id)` on SQLite/Postgres), never iterating or decoding payloads, so it is safe to run against a live deployment. `--stream-id` narrows the report to one stream (showing a zero row when that stream is absent); output is a table by default or `--json` for scripting and dashboards. An unreachable store prints a single error line and exits non-zero rather than a traceback. `last id` is the never-expired counter on Redis or the highest stored ID on SQLite/Postgres (which can trail the sequence after a purge).

## [1.8.1] - 2026-06-10

### Security
- **Proxy no longer replays another session's events from a guessed `Last-Event-ID`.** `PersistenceProxy` assigns sequential, trivially enumerable event IDs, and its GET reconnect path resolved the stream to replay from the **global** event ID without checking that the stream belonged to the requesting session. A client could send `GET /mcp` with `Last-Event-ID: <n>` (and any/no `mcp-session-id`) and receive the buffered history of whatever session owned event `n`: a cross-session information disclosure. Replay is now gated on session ownership: the proxy names every stream `f"{session_id}:..."`, and a `Last-Event-ID` whose resolved stream does not carry the requesting session's prefix replays nothing (the client still resumes live notifications for its own session). The same ownership check was added to `StreamBuffer.consume_from`'s cold-replay path, so a live GET buffer can't be steered into replaying a foreign stream either. A blocked attempt is logged at `WARNING`.
- **Decompression is now bounded (decompression-bomb guard).** `decompress_payload` inflated `gz:`-marked payloads with an unbounded `gzip.decompress`. A crafted payload (reachable by anything with direct write access to the backing store, such as a shared Redis/Postgres or a restored dump) could expand to gigabytes and OOM a worker on replay. Inflation now runs incrementally against a 100 MiB output cap (`compression.MAX_DECOMPRESSED_BYTES`) and raises `ValueError` past it; callers already skip an undecodable event, so a bomb costs one skipped event rather than the process. Legitimate JSON-RPC payloads are orders of magnitude under the cap and are unaffected.
- **Proxy bounds request-body buffering.** The proxy must read each request fully before forwarding it (to recompute `Content-Length`), so an unbounded body let a single large POST exhaust memory. `PersistenceProxy` (and `PersistenceProxy.create`) now take `max_request_body_bytes` (default 10 MiB); a body over the limit is rejected with `413 Request Entity Too Large` and never fully buffered.

## [1.8.0] - 2026-06-09

### Added
- **`PurgeScheduler` jitter** (`jitter=`): an optional non-negative number of seconds added to each purge cycle, drawn fresh from `[0, jitter]` every iteration, so replicas that start together from the same rolling deploy stop purging a shared backend in lockstep (a "thundering herd"). A good rule of thumb is 10 to 20 percent of `interval` (e.g. `interval=300, jitter=30` spreads replicas across a 30s window). Validated non-negative at construction. Defaults to `0.0`, keeping the loop exactly periodic, so existing behavior is unchanged. Documented in the production guide under [reclaiming space](docs/production.md#2-reclaiming-space-schedule-purge_expired).
- **TypeScript / non-Python proxy guide** ([`docs/typescript.md`](docs/typescript.md)): a step-by-step guide to putting `mcp-persist-proxy` in front of an MCP server written in another language. The proxy speaks plain HTTP, so it adds SSE resumability without importing anything into the upstream process. Covers both CLI modes (point at a running server, or launch it as a subprocess), the Streamable HTTP transport requirement, backend choice and `ttl` sizing, and the shared-store rule for multiple proxy replicas. Linked from the README proxy section.
- **`examples/resume_demo.py`**: a self-contained, recordable terminal demo of resumability in a single command (`python examples/resume_demo.py`). It runs a real FastMCP + `SQLiteEventStore` server in a background thread, starts a streaming tool call, yanks the connection mid-stream to simulate a client/network crash, then reconnects with `Last-Event-ID` and watches the server replay exactly the missed events before delivering the tool's result. Nothing is mocked: events round-trip through SQLite (`resume_demo.db`) and the client speaks the Streamable HTTP wire protocol, parsing the SSE stream with mcp-persist's own `SSEParser`. Needs only the `[sqlite]` extra (uvicorn + httpx already ship with `mcp`).

### Changed
- README now reflects the current test suite size (300+ async tests across all three backends), replacing the stale earlier count.
- README now carries a PyPI downloads badge ([pepy.tech](https://pepy.tech/project/mcp-persist)) alongside the existing CI, version, Python, and license badges.

## [1.7.0] - 2026-06-05

### Added
- **Persistence proxy** (`mcp_persist.PersistenceProxy` + the `mcp-persist-proxy` CLI):
  - An ASGI app that adds SSE stream resumability in front of **any** upstream MCP server without modifying it. It forwards requests upstream and, for `text/event-stream` responses, intercepts the stream: each event is parsed, persisted to an `EventStore` (which assigns the proxy's own monotonic event ID), and forwarded to the client. A client that drops can reconnect with `Last-Event-ID`; the proxy replays the missed events from the store and then continues live. The upstream runs **without** its own event store: the proxy is the store. The buffer outlives the client request, so a disconnect mid-response keeps storing and a later reconnect gets a complete history.
  - Scope is honest: it survives **client** disconnects against a stable upstream. It does not survive an upstream restart (new session, new IDs), and an event that neither the client nor the proxy received before storage is gone: same at-most-once guarantee as the SDK itself.
  - Store resolution mirrors `with_persistence`: `PersistenceProxy.create(upstream, store=...)` (caller-owned), `backend=`+`url=`(+`ttl=`) built and closed for you, or neither → `event_store_from_env()` (`MCP_PERSIST_*`). `create()` owns the shared `httpx.AsyncClient` and the store/buffer lifecycle.
  - Follows upstream redirects internally (e.g. a server's `/mcp` → `/mcp/` trailing-slash redirect), so the client sees a clean endpoint and never gets bounced past the proxy to the upstream.
  - **CLI** (`mcp-persist-proxy`): point at a running upstream (`--upstream URL --backend sqlite --url events.db [--port 8000] [--path /mcp]`), or start one as a subprocess, wait for it to come up, and proxy it (`--backend redis --url ... [--upstream-port 8001] -- uvicorn my_server:app --port 8001`); the child is stopped (SIGTERM, then SIGKILL) when the proxy exits.
  - **No new dependencies**: `httpx` and `uvicorn` already ship transitively with `mcp`, so the proxy needs no extra install. New modules `mcp_persist/proxy.py`, `mcp_persist/_stream_buffer.py`, `mcp_persist/_sse_parser.py`, `mcp_persist/_cli.py`, with unit tests for the SSE parser, the stream buffer (cold/hot replay, live fan-out, disconnect survival), the proxy (JSON passthrough, POST/GET SSE, reconnect to a live buffer vs. store-only replay, mid-stream disconnect), and CLI argument handling.
  - Documented in the README ("Resumability without touching the server") and the [production guide](docs/production.md) (proxy mode: single point of failure, the shared-store requirement across proxy replicas, and the upstream-restart scope boundary).

## [1.6.0] - 2026-06-04

### Added
- **FastMCP plugin** (`mcp_persist.with_persistence`):
  - Takes a `FastMCP` instance and returns a runnable Starlette ASGI app with SSE stream resumability already wired in, collapsing the manual dance of opening a connection, building an `EventStore`, constructing a `StreamableHTTPSessionManager`, and writing a Starlette lifespan down to a single call. The returned app owns the store + session-manager lifecycle (opened on startup, closed on shutdown) and mounts the MCP endpoint at `mcp_path` (default `/mcp`).
  - Three ways to supply the store, resolved in order: a pre-built `store=` (caller owns its lifecycle, **not** closed on shutdown); `backend=` + `url=` config kwargs (`ttl`/`table_name` for sqlite & postgres, `key_prefix`/`max_stream_length` for redis), built and closed for you; or neither, falling back to `event_store_from_env()` (`MCP_PERSIST_*`). Conflicting or inapplicable arguments (e.g. `store=` together with `backend=`, a redis-only option on sqlite, or config kwargs with no backend) raise `ValueError` rather than being silently ignored. The live store is exposed on `app.state.event_store` so a `PurgeScheduler` can run alongside the server.
  - **No new dependencies**: `starlette` and `StreamableHTTPSessionManager` already ship with `mcp`. New example `examples/fastmcp_plugin_server.py` and end-to-end tests in `tests/test_fastmcp_plugin.py` (real MCP client over an in-process server, asserting events persist and replay through the plugin-wired store).

## [1.5.0] - 2026-06-04

### Added
- **SQLite write-behind commits** (`commit_interval`, `commit_max_pending`):
  - `SQLiteEventStore` (and `SQLiteEventStore.create()`) accept an optional `commit_interval` (seconds). When set, `store_event` no longer commits on every call; instead the insert stays in SQLite's open transaction and a background task commits all buffered events every `commit_interval` seconds: one `fsync` per interval instead of one per event, trading a bounded durability window for substantially higher write throughput. Buffered events remain **immediately visible to `replay_events_after` and `subscribe` on the same store** (read-your-writes within the process); only a hard crash loses the uncommitted tail (≤ one interval). `commit_max_pending` additionally commits inline once that many events are buffered, bounding the loss window by count and capping the open transaction size under bursts; it can be used alone for pure count-based group commit. Both default to `None`, so the existing durable commit-per-event behavior is unchanged.
  - New lifecycle: `SQLiteEventStore` is now an async context manager and exposes `await store.aclose()`, which stops the flusher and commits the final batch. **Write-behind requires closing the store** (via `create()`, `async with store:`, or `aclose()`) or the last interval of events is dropped on shutdown; `create()`'s context manager flushes and closes for you. `aclose()` is idempotent and a no-op when write-behind is off.
  - Docs: new "Write-behind commits (SQLite)" sections in the README and `docs/production.md` (durability trade-off, the mandatory-close footgun, the single-writer caveat, and a production-checklist item).

### Fixed
- **Corrupt or partial compressed payloads no longer crash replay, `subscribe`, or migration.** A `gz:`-marked payload that was truncated or otherwise not valid gzip/base64 raised `gzip.BadGzipFile` / `binascii.Error` from `decompress_payload`, which escaped the `ValidationError`-only guard and aborted the entire stream (or migration run) instead of skipping the one bad event. All three backends now skip an undecodable event (logging a warning with the underlying error) and continue, exactly as they already tolerated a single payload that failed JSON-RPC validation. Regression coverage in `tests/test_bug_finding_corruption.py`, plus new stress (`tests/test_stress_*.py`) and integrity (`tests/test_integrity_all.py`) suites.

## [1.4.0] - 2026-06-03

### Added
- **Payload compression** (`compression`, `compress_min_bytes`):
  - All three stores accept `compression="gzip"` to gzip-compress event payloads above `compress_min_bytes` (default `1024`) before storing them, cutting storage and (on Redis) memory for large tool results / JSON-RPC bodies. The stored form is marker-prefixed (`gz:` + base64), so it can never collide with a real payload, and **decompression on read is automatic and independent of the setting**: a store reads compressed payloads written by another store even with compression disabled, so the option is safe to roll out incrementally and across `migrate()`. Compression is only kept when it actually shrinks the payload, so small/incompressible messages are never made larger. Default is `None` (no compression); existing data is unaffected.
- **Batched purge** (`purge_expired(batch_size=...)`):
  - `SQLiteEventStore.purge_expired` and `PostgresEventStore.purge_expired` now accept an optional `batch_size`. When set, expired rows are deleted in bounded chunks (SQLite via an indexed `event_id` subselect; Postgres via a `ctid` subselect) committing per chunk, so a large purge does not hold one long lock that contends with live inserts and replay scans. The expiry cutoff is captured once up front. `batch_size=None` (the default) keeps the previous single-statement `DELETE`.
- **Replay gap detection**:
  - `replay_events_after` now logs a `WARNING` when the anchor (`Last-Event-ID`) still exists but one or more events after it have expired / are missing and will be silently skipped, surfacing an otherwise invisible, unrecoverable gap to a reconnecting client. SQLite/Postgres detect this with a `ttl`-gated existence check; Redis warns when stream-index entries after the anchor have lost their payloads. `replay_events_after` still returns normally and delivers every event it can.
- **`ping()` readiness probe**:
  - All three stores expose `async ping() -> bool` (Redis `PING`, Postgres/SQLite `SELECT 1`) for liveness/readiness probes. Returns `True` on success and lets connection errors propagate so a probe can treat a raised exception as "not ready".
- **`PurgeScheduler`**:
  - A batteries-included async context manager / `start()`+`aclose()` wrapper that runs `purge_expired()` on an interval (with optional `batch_size`), logs `purged N events`, and swallows transient errors so the loop survives a backend blip. Rejects stores without `purge_expired` (e.g. `RedisEventStore`, which expires keys natively) at construction. Replaces the hand-rolled purge-loop snippet from `docs/production.md`. Exported from `mcp_persist`.
- **`event_store_from_env()`**:
  - Builds a store from `MCP_PERSIST_BACKEND` / `MCP_PERSIST_URL` (+ optional `MCP_PERSIST_TTL`, `MCP_PERSIST_TABLE_NAME`, `MCP_PERSIST_KEY_PREFIX`, `MCP_PERSIST_MAX_STREAM_LENGTH`) and returns the matching backend's `create()` context manager, so a deployment can pick its store from config without branching on the backend. Exported from `mcp_persist`.
- **Tooling & docs**:
  - `compose.yaml` at the repo root spins up local Redis + Postgres for the examples and for running the test suite against real backends.
  - New `docs/production.md` material: deployment topologies (rolling deploys / load balancers without sticky sessions, serverless / read-only filesystems), the scope boundary of resumability ("what it does *not* give you"), the Redis monotonic-counter throughput ceiling, and guidance for the new features above.
  - **Multi-process integration test**: a separate OS process writes events to a shared Redis/Postgres while the test process concurrently replays them, validating cross-process resumability (the reason to choose Redis/Postgres over single-writer SQLite). Gated on backend availability.

## [1.3.0] - 2026-06-01

### Changed
- **RedisEventStore**: now logs a warning at construction when `max_stream_length` is set together with `ttl=None`. Trimming the stream index to `max_stream_length` drops old event IDs but does not delete their payload hashes, so without a `ttl` those payloads never expire and accumulate in Redis indefinitely. Pair `max_stream_length` with a `ttl` so trimmed payloads expire on their own.

### Added
- **Push-based streaming**:
  - `subscribe(stream_id)` async generator on all three backends: yields `(event_id, message)` for events as they are written, instead of polling `replay_events_after`. Backed by Redis pub/sub, Postgres `LISTEN`/`NOTIFY`, and an SQLite polling fallback (`poll_interval`, default 0.5s). Opt in with the new `enable_streaming=True` constructor flag; with the default `False` there is no extra per-write round-trip and `subscribe()` raises, so existing behavior is unchanged. Delivery is **best-effort and forward-only (at-most-once)**: only events written after the subscription registers are delivered, the notification publish is best-effort (a failure is logged, never failing the write), and `replay_events_after` remains the durable catch-up path. Subscriptions are cancellable and release their connection on teardown. See the new "Real-time streaming with `subscribe()`" and "Subscribers and connection pools" sections in `docs/production.md` (each Postgres subscriber holds a pool connection for its lifetime; size the pool accordingly).
- **Cross-backend migration**:
  - `migrate(source, dest)`: copies every event from one store to another, preserving per-stream ordering and payloads. Supports `stream_id=` scoping to a single stream, an `on_progress` callback, and a configurable `batch_size`. Streams are migrated independently and returned in a `MigrationResult` (`streams_migrated`, `events_migrated`, `failed_streams`); a stream that errors is logged and skipped rather than aborting the run. Priming (empty-payload) events are copied faithfully. Note: event IDs and `created_at` timestamps are **not** preserved (the destination issues fresh IDs and timestamps), so client `Last-Event-ID` resumability tokens are invalidated by a migration; see the new "Migrating between backends" section in `docs/production.md`. `migrate` and `MigrationResult` are exported from `mcp_persist`.
  - `list_streams()` on all three backends: yields each distinct stored stream ID; backs whole-database migration.
- **Metrics / observability**:
  - `MetricsCollector` protocol with optional `metrics=` hooks on all three stores. Implement `on_store_event(stream_id, event_id, duration_ms)`, `on_replay(stream_id, events_replayed, duration_ms)`, and `on_error(operation, error)` to emit timing and count data to Prometheus, Datadog, logs, or anything else. When no collector is supplied the store uses a `NoOpMetricsCollector` and takes a fast path with no measurable overhead. Hook calls are isolated: a collector that raises is logged and ignored, never turning a successful store or replay into a failure. Ships with `LoggingMetricsCollector` (one `DEBUG` line per operation) built in. `MetricsCollector`, `NoOpMetricsCollector`, and `LoggingMetricsCollector` are exported from `mcp_persist`.
- **RedisEventStore, SQLiteEventStore & PostgresEventStore**:
  - `create()` classmethod: an async context manager that owns the connection lifecycle, so callers no longer have to construct and tear down the underlying client/connection/pool themselves:
    ```python
    async with RedisEventStore.create("redis://localhost:6379", ttl=3600) as store:
        await store.store_event(...)
    ```
    `RedisEventStore.create(url, ...)` opens the client via `redis.asyncio.from_url`; `SQLiteEventStore.create(path, ...)` opens an `aiosqlite` connection and calls `initialize()`; `PostgresEventStore.create(dsn, ...)` opens an `asyncpg` pool and calls `initialize()`. The connection is always closed on exit, including when `initialize()` or the body raises. Store options (`ttl`, `key_prefix`/`table_name`, etc.) are keyword arguments; any extra keyword arguments are forwarded to the underlying driver (`from_url` / `connect` / `create_pool`). The driver is imported lazily inside `create()`, so importing `mcp_persist` still works without the optional backend installed.

## [1.2.1] - 2026-05-30

### Added
- **PostgresEventStore**:
  - `replay_batch_size` constructor parameter (default 500) to tune how many rows are fetched per round-trip during replay (useful for deployments with unusually large payloads, and previously only adjustable by monkey-patching a private module constant).
- **Documentation**:
  - "Redis connection pool sizing" section in `docs/production.md` explaining that `redis.asyncio` defaults to `max_connections=100` and raises `MaxConnectionsError` when the pool is exhausted (unlike `asyncpg`, which queues), with `max_connections` / `BlockingConnectionPool` remedies for high SSE fan-out.

## [1.2.0] - 2026-05-30

### Fixed
- **RedisEventStore, SQLiteEventStore & PostgresEventStore**:
  - `replay_events_after` no longer aborts the entire stream when it encounters a single event with a corrupt or unparseable payload. The offending event is now logged at `WARNING` and skipped, so a reconnecting client still receives every other event on the stream instead of losing the whole replay to one malformed row.

### Added
- **Tests**:
  - Regression tests for all three backends asserting that a corrupt payload injected mid-stream is skipped during replay while the events stored before and after it are still delivered.

## [1.1.4] - 2026-05-30

### Fixed
- **Tests**:
  - Add database and table cleanups (flushing Redis and dropping Postgres tables) at the end of the example integration tests to prevent state leakage to unit tests in CI environment.

### Added
- **Documentation**:
  - Code snippet in `docs/production.md` demonstrating python logging configuration details for all `mcp_persist.*` backends.

## [1.1.3] - 2026-05-30

### Changed
- **Tests**:
  - Parameterized example server integration test to run SQLite, Redis, and Postgres servers and check them with the client smoke test.
  - Automatically skip Redis and Postgres examples smoke tests locally when backend services are not running, while running them fully in CI.

## [1.1.2] - 2026-05-30

### Added
- **Tests**:
  - Integration smoke test that executes the full example SQLite server and verifies it against the client smoke test.

### Changed
- **Documentation**:
  - Reorganized benchmark results in the README into two clean tables showing storage performance and multi-scale replay performance separately.
  - Expanded production guide for explicit log configuration and SDK error monitoring of the `mcp_persist.*` loggers.

## [1.1.1] - 2026-05-30

### Fixed
- **Benchmarks**:
  - Code formatting standardizations for the benchmarks script.

## [1.1.0] - 2026-05-30

### Added
- **Benchmarks**:
  - Separate benchmarks measuring replay latency at multiple scales (100, 1,000, and 10,000 events) comparing SQLite, Redis, and Postgres.
- **Documentation**:
  - Dedicated "Architecture & Guarantees" section in the README explaining event ordering, concurrency/write semantics, and consistency/durability levels of each store.
  - Updated Redis replay comparison notes to reflect pipeline optimizations.

## [1.0.4] - 2026-05-30

### Added
- **PostgresEventStore & SQLiteEventStore**:
  - Double-quoted table and index names to allow hyphens and other valid non-standard SQL identifiers (e.g. `mcp-events`).
  - Validation pattern `^[a-zA-Z0-9_-]+$` replacing the Python-specific `isidentifier()` restriction.
- **SQLiteEventStore**:
  - In-process `self._init_lock` to serialize concurrent database first-time setup operations, consistent with the Postgres backend.
- **Tests**:
  - Stress tests in all backends that concurrently write to a stream and replay events from it, validating ordering and correctness under load.
- **Documentation**:
  - Simplified ASCII architecture diagram and earlier core purpose explanation in `README.md`.
  - Detailed production guide for Redis memory scaling under high stream cardinality (millions of unique stream IDs) and recommended eviction policies.
  - Documented system and software specs for the published benchmarks.

## [1.0.3] - 2026-05-30

### Added
- **RedisEventStore**:
  - `max_stream_length` constructor parameter to trim and bound the size of the stream's sorted set, preventing unbounded memory leak on active streams.
  - Normalization of Redis replies allowing full support for Redis clients configured with `decode_responses=True`.
- **PostgresEventStore & SQLiteEventStore**:
  - Index on `created_at` created during schema initialization to avoid sequential scans / table scans during `purge_expired()`.
  - Schema-qualified table name support (e.g. `schema.table` / `public.mcp_events`).
  - `timeout` constructor parameter for database busy/lock timeouts.
  - Streaming and batching during replay to avoid Out-Of-Memory (OOM) failures under massive event backlogs.

### Fixed
- **RedisEventStore**:
  - Pipeline switched to `transaction=False` to prevent `CROSSSLOT` execution failures on Redis Cluster environments.
  - Replay performance optimized by batching payload fetches into a single pipelined execute call rather than a sequential network loop.
  - Added lazy pruning of stale event IDs from the stream's sorted set during replay.
- **PostgresEventStore & SQLiteEventStore**:
  - Wrapped DDL creation queries in exception handlers that safely tolerate concurrent catalog registration races when multiple workers or replicas initialize at the same instant.

## [1.0.2] - 2026-05-30

### Fixed
- `replay_events_after` now returns `None` for a non-numeric `Last-Event-ID`
  instead of raising `ValueError`. The SDK passes this client-controlled header
  through unvalidated; previously SQLite and Postgres raised on `int()` (logging
  a traceback and aborting the replay) while Redis tolerated it. All three
  backends now handle it uniformly.
- Corrected the README and module-docstring quickstarts: they passed
  `app=mcp_server` (undefined, and the wrong type): `StreamableHTTPSessionManager`
  needs the low-level server, so they now show `app=mcp._mcp_server` from a
  `FastMCP` instance, matching the runnable examples.

### Added
- `docs/production.md`: a production deployment guide (scheduling
  `purge_expired()`, failure modes, schema and permissions, security, scaling,
  observability), linked from the README.

### Changed
- Removed the redundant `aiosqlite` line from the SQLite example's install
  instructions (the `[sqlite]` extra already provides it).

### Tests
- Added a non-numeric `Last-Event-ID` replay test to all three backend suites.

## [1.0.1] - 2026-05-30

### Fixed
- `RedisEventStore` no longer sets a TTL on the counter key. Previously the
  counter expired along with events, so after an idle period longer than `ttl`
  the next event ID restarted from `1`, breaking the monotonic-ID guarantee. The
  counter now persists for the life of the Redis instance, matching the
  `AUTOINCREMENT` / `IDENTITY` sequences of the SQLite and Postgres backends.
- `PostgresEventStore.initialize` is now guarded by a lock and short-circuits
  once initialized, so concurrent first `store_event` calls on a pool can no
  longer race on `CREATE TABLE IF NOT EXISTS` (which can raise a duplicate-key
  error on Postgres system catalogs).

### Added
- `mcp_persist.__version__`, resolved from installed package metadata.

### Changed
- `RedisEventStore.store_event` now writes the event hash, its sorted-set entry,
  and their TTLs in a single transactional pipeline instead of separate
  round-trips. This makes the per-event write atomic (a mid-write crash can no
  longer orphan an event hash or leave a key without its expiry) and removes
  round-trips from the hot path.
- README: explained SQLite's throughput advantage (no network hop) and its
  single-writer caveat; surfaced the Redis per-event replay cost (`O(log N + M)`,
  one round-trip per replayed event) in the `RedisEventStore` "How it works"
  section; spelled out the consequence of multi-process SQLite access
  (`SQLITE_BUSY` / "database is locked").
- Documentation: added `PostgresEventStore` to the `CONTRIBUTING.md` intro, the
  bug-report issue template, and the pull-request checklist.

### Tests
- Corrected the Redis counter-TTL test to assert the counter has no expiry
  (was asserting the buggy behavior), clarified that the SQLite concurrent-ID
  test passes only because aiosqlite serializes writes through one connection,
  and added a package-level test covering `__version__` and the public exports.

## [1.0.0] - 2026-05-27

First stable release. The three backends (`RedisEventStore`, `SQLiteEventStore`,
`PostgresEventStore`) and their public API are now considered stable; future
breaking changes will follow semantic versioning with a major version bump.

### Added
- `benchmarks/benchmark.py` comparing `store_event` latency/throughput and
  `replay_events_after` latency across all three backends.
- "Choosing a backend" section in the README with a decision guide and
  comparison table, plus a benchmarks summary.

### Changed
- Clarified the `PostgresEventStore.purge_expired` docstring with a one-line
  explanation of `pg_cron`.

## [0.3.0] - 2026-05-27

### Added
- `PostgresEventStore`: PostgreSQL-backed `EventStore` (via `asyncpg`) for
  durable SSE resumability on deployments already running Postgres, including
  multi-node / team setups. Install with the `postgres` extra.
- Example MCP server `examples/postgres_server.py`.
- `py.typed` marker so downstream type checkers use the bundled type hints (PEP 561).

### Removed
- The published `dev` extra. Development dependencies now live in a PEP 735
  `[dependency-groups]` table, so `pip install "mcp-persist[dev]"` is no longer
  available; contributors use `uv sync --dev` instead. The `redis` and `sqlite`
  extras are unchanged.

## [0.2.0] - 2026-05-27

### Added
- `SQLiteEventStore`: SQLite-backed `EventStore` for single-node SSE
  resumability that survives process restarts, with no external service.
- Example MCP servers for both backends under `examples/`.

## [0.1.1] - 2026-05-26

### Fixed
- Broken import that made the package unimportable on current `mcp` releases.

### Changed
- Restored the `src/` layout.

## [0.1.0] - 2026-05-26

### Added
- Initial release with `RedisEventStore`, a Redis-backed `EventStore` for
  multi-worker / multi-process SSE resumability.

[2.1.1]: https://github.com/Ar-maan05/mcp-persist/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/Ar-maan05/mcp-persist/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.12.3...v2.0.0
[1.12.3]: https://github.com/Ar-maan05/mcp-persist/compare/v1.12.2...v1.12.3
[1.12.2]: https://github.com/Ar-maan05/mcp-persist/compare/v1.12.1...v1.12.2
[1.12.1]: https://github.com/Ar-maan05/mcp-persist/compare/v1.12.0...v1.12.1
[1.12.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.11.1...v1.12.0
[1.11.1]: https://github.com/Ar-maan05/mcp-persist/compare/v1.11.0...v1.11.1
[1.11.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.10.0...v1.11.0
[1.10.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.9.0...v1.10.0
[1.9.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.8.4...v1.9.0
[1.8.4]: https://github.com/Ar-maan05/mcp-persist/compare/v1.8.3...v1.8.4
[1.8.3]: https://github.com/Ar-maan05/mcp-persist/compare/v1.8.2...v1.8.3
[1.8.2]: https://github.com/Ar-maan05/mcp-persist/compare/v1.8.1...v1.8.2
[1.8.1]: https://github.com/Ar-maan05/mcp-persist/compare/v1.8.0...v1.8.1
[1.8.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/Ar-maan05/mcp-persist/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.4...v1.2.0
[1.1.4]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.3...v1.1.4
[1.1.3]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/Ar-maan05/mcp-persist/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.4...v1.1.0
[1.0.4]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/Ar-maan05/mcp-persist/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/Ar-maan05/mcp-persist/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/Ar-maan05/mcp-persist/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Ar-maan05/mcp-persist/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/Ar-maan05/mcp-persist/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Ar-maan05/mcp-persist/releases/tag/v0.1.0
