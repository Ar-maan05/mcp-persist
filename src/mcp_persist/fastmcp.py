"""MCPServer plugin for mcp-persist.

Wire SSE stream resumability into a :class:`~mcp.server.mcpserver.MCPServer`
with a single call. :func:`with_persistence` takes the ``MCPServer`` instance and
returns a runnable Starlette ASGI app with a
:class:`~mcp.server.streamable_http_manager.StreamableHTTPSessionManager`
already wired to an :class:`~mcp.server.streamable_http.EventStore`, managing the
store and manager lifecycle for you via the app's lifespan.

Three ways to supply the store, in resolution order:

Pattern A — config kwargs (most common)::

    from mcp.server.mcpserver import MCPServer
    from mcp_persist import with_persistence

    mcp = MCPServer(name="MyServer")
    app = with_persistence(mcp, backend="sqlite", url="events.db", ttl=3600)
    # `app` is a Starlette ASGI app — run it with uvicorn:
    #   uvicorn.run(app, host="127.0.0.1", port=8000)

Pattern B — a pre-built store (caller owns its lifecycle)::

    async with SQLiteEventStore.create("events.db", ttl=3600) as store:
        app = with_persistence(mcp, store=store)
        # the app uses `store` but does NOT close it; the `async with` does.

Pattern C — env-driven (12-factor)::

    # export MCP_PERSIST_BACKEND=redis MCP_PERSIST_URL=redis://... MCP_PERSIST_TTL=3600
    app = with_persistence(mcp)  # reads MCP_PERSIST_* via event_store_from_env()

No new dependencies: ``starlette`` and the session manager ship with ``mcp``.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.applications import Starlette
from starlette.routing import Mount

from mcp_persist.config import build_store_context, env_flag, event_store_from_env
from mcp_persist.recorder import DEFAULT_MAX_QUEUE
from mcp_persist.records import PayloadPolicy
from mcp_persist.session_scope import SessionScopedSessionManager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AbstractAsyncContextManager

    from mcp.server.mcpserver import MCPServer
    from mcp.server.streamable_http import EventStore

    from mcp_persist.encryption import KeyRing
    from mcp_persist.metrics import MetricsCollector
    from mcp_persist.records import RecordStore
_BACKENDS = ("sqlite", "redis", "postgres")


def with_persistence(
    mcp: MCPServer,
    store: EventStore | None = None,
    *,
    backend: str | None = None,
    url: str | None = None,
    ttl: int | None = None,
    table_name: str | None = None,  # sqlite / postgres
    key_prefix: str | None = None,  # redis
    max_stream_length: int | None = None,  # redis
    tenant_id: str | None = None,
    compression: str | None = None,
    compress_min_bytes: int = 1024,
    keyring: KeyRing | None = None,
    batch_max_events: int | None = None,
    batch_max_latency_ms: float | None = None,
    session_idle_timeout: float | None = None,
    durable_sessions: bool | None = None,
    session_table_name: str | None = None,
    record: bool | None = None,
    record_store: RecordStore | None = None,
    record_table_name: str | None = None,
    record_ttl: int | None = None,
    record_payload_policy: PayloadPolicy | None = None,
    record_max_queue: int = DEFAULT_MAX_QUEUE,
    record_metrics: MetricsCollector | None = None,
    record_allow_plaintext: bool = False,
    warn_on_bypass: bool = True,
    mcp_path: str = "/mcp",
) -> Starlette:
    """Return a Starlette ASGI app serving ``mcp`` with SSE resumability.

    The returned app mounts the MCP endpoint at ``mcp_path`` (default ``/mcp``)
    and, through its lifespan, opens the event store, runs a
    ``StreamableHTTPSessionManager`` bound to it, and tears both down on
    shutdown. Pass it straight to uvicorn, or mount/compose it in a larger
    Starlette app.

    The store is chosen by the first of these that is set:

    1. ``store=`` — used as-is; the caller owns its lifecycle (it is not
       closed on app shutdown). Passing ``store=`` together with ``backend=`` or
       ``url=`` is an error.
    2. ``backend=`` (+ ``url=``) — built via the backend's ``create()`` context
       manager and closed on app shutdown. All store options accepted by the
       shared factory are available here, including tenancy, compression,
       encryption, and Redis/Postgres batching.
       Passing an option that does not apply to the chosen backend is an error.
    3. neither — falls back to :func:`~mcp_persist.event_store_from_env`, which
       reads ``MCP_PERSIST_*`` from the environment. In this case passing any of
       ``url``/``ttl``/``table_name``/``key_prefix``/``max_stream_length`` is an
       error, since configuration comes from the environment.

    Args:
        mcp: The ``MCPServer`` to serve.
        store: A pre-built event store (Pattern B). Mutually exclusive with
            ``backend``/``url``.
        backend: ``"sqlite"``, ``"redis"`` or ``"postgres"`` (Pattern A).
        url: Path / URL / DSN for the backend (required with ``backend``).
        ttl: Event ttl in seconds (sqlite / redis / postgres).
        table_name: Table name (sqlite / postgres).
        key_prefix: Redis key prefix.
        max_stream_length: Redis per-stream cap.
        session_idle_timeout: Optional idle timeout in seconds for stateful
            sessions, forwarded to ``StreamableHTTPSessionManager``.
        durable_sessions: Record session ids in the store alongside the events,
            and resume a session this process did not create instead of
            answering 404. This is what makes resumability survive a restart or
            a worker without sticky routing; see
            :mod:`mcp_persist.session_manager`. Left unset it reads
            ``MCP_PERSIST_DURABLE_SESSIONS`` and otherwise defaults to False,
            which keeps the upstream behaviour exactly.
        session_table_name: Table for the session registry on the SQL backends
            (default ``"mcp_sessions"``). Requires ``durable_sessions=True``.
        record: Write a durable record of every handled message. Unlike the
            event store this works on **every** protocol version, including the
            stateless 2026-07-28 transport where the event store is never
            consulted, so it is what keeps a deployment observable across a
            protocol migration. Defaults to False (reading
            ``MCP_PERSIST_RECORD``), because recording changes what an existing
            deployment writes.
        record_store: A caller-owned record store. Required when ``store=`` was
            supplied and ``record=True``: a record store is never inferred from
            another store's internals, since that would silently pick a backend
            the caller did not choose.
        record_table_name: Table for records on the SQL backends (default
            ``"mcp_records"``).
        record_ttl: Redis-only record retention in seconds. Separate from
            ``ttl``, since records usually outlive the events they describe.
        record_payload_policy: Which params may be captured. Defaults to
            capturing none. See :class:`~mcp_persist.PayloadPolicy`.
        record_max_queue: How many records may await writing before further
            records are dropped and counted. Records never block a request.
        record_metrics: Collector receiving ``on_record_write`` /
            ``on_record_drop`` / ``on_error``, so record loss is visible to the
            same monitoring as everything else. The hooks are feature-detected,
            so a collector written before records existed works unchanged.
        record_allow_plaintext: Accept payload capture into a store with no
            keyring. Off by default, so capturing params without encryption is
            a deliberate choice rather than an accident.
        warn_on_bypass: Log once per protocol version when requests take the
            stateless transport and therefore never reach the event store.
        mcp_path: Mount path for the MCP endpoint (default ``"/mcp"``).
    """
    if durable_sessions is None:
        durable_sessions = env_flag("MCP_PERSIST_DURABLE_SESSIONS")
    if session_table_name is not None and not durable_sessions:
        raise ValueError("with_persistence: session_table_name requires durable_sessions=True")
    if record is None:
        record = env_flag("MCP_PERSIST_RECORD")
    records = _RecordOptions(
        enabled=record,
        store=record_store,
        table_name=record_table_name,
        ttl=record_ttl,
        policy=record_payload_policy,
        max_queue=record_max_queue,
        metrics=record_metrics,
        allow_plaintext=record_allow_plaintext,
    )
    records.validate(store_supplied=store is not None, backend=backend)
    records.check_encryption_eagerly(backend=backend, keyring=keyring)
    ctx, owned_store = _resolve_store(
        store,
        backend=backend,
        url=url,
        ttl=ttl,
        table_name=table_name,
        key_prefix=key_prefix,
        max_stream_length=max_stream_length,
        tenant_id=tenant_id,
        compression=compression,
        compress_min_bytes=compress_min_bytes,
        keyring=keyring,
        batch_max_events=batch_max_events,
        batch_max_latency_ms=batch_max_latency_ms,
    )
    opts = _SessionOptions(durable=durable_sessions, table_name=session_table_name)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        # ctx is set when we own the store's lifecycle (Patterns A & C); enter it
        # so the store is opened on startup and closed on shutdown. When a store
        # was passed in (Pattern B), ctx is None and we leave it untouched.
        if ctx is not None:
            async with ctx as resolved_store:
                async with _run_recording(app, mcp, resolved_store, records, warn_on_bypass):
                    async with _run_manager(app, mcp, resolved_store, session_idle_timeout, opts):
                        yield
        else:
            assert owned_store is not None
            async with _run_recording(app, mcp, owned_store, records, warn_on_bypass):
                async with _run_manager(app, mcp, owned_store, session_idle_timeout, opts):
                    yield

    return Starlette(lifespan=lifespan, routes=[Mount(mcp_path, app=_handle_mcp)])


@dataclass(frozen=True)
class _SessionOptions:
    durable: bool
    table_name: str | None


@dataclass(frozen=True)
class _RecordOptions:
    enabled: bool
    store: RecordStore | None
    table_name: str | None
    ttl: int | None
    policy: PayloadPolicy | None
    max_queue: int
    metrics: MetricsCollector | None
    allow_plaintext: bool

    def validate(self, *, store_supplied: bool, backend: str | None = None) -> None:
        """Reject record configuration that could not take effect."""
        if not self.enabled:
            stray = _names_set(
                record_store=self.store,
                record_table_name=self.table_name,
                record_ttl=self.ttl,
                record_payload_policy=self.policy,
                record_metrics=self.metrics,
            )
            if self.allow_plaintext:
                stray.append("record_allow_plaintext")
            if stray:
                raise ValueError(f"with_persistence: {', '.join(sorted(stray))} require record=True")
            return
        if store_supplied and self.store is None:
            # Inferring a record store from the event store's private internals
            # would silently choose a backend the caller never asked for, which
            # is exactly the class of config drift this project has been bitten
            # by before. Make the caller say it.
            raise ValueError(
                "with_persistence: record=True with a caller-supplied store= also needs record_store=, "
                "since the record store is never inferred from another store's internals"
            )
        if self.store is not None and self.table_name is not None:
            raise ValueError("with_persistence: record_table_name does not apply when record_store= is supplied")
        if self.ttl is not None and self.store is not None:
            raise ValueError(
                "with_persistence: record_ttl does not apply when record_store= is supplied; the "
                "caller-built store already carries its own retention"
            )
        if self.ttl is not None and not self._ttl_backend_is_redis(backend):
            # Only Redis expires keys natively. Accepting the option anywhere
            # else and quietly doing nothing would be a retention setting that
            # looks configured and is not, which is precisely the silent failure
            # this release exists to remove. Anything that is not provably Redis
            # is rejected, including an environment-resolved backend, because
            # "probably fine" is how a no-op ships.
            raise ValueError(
                "with_persistence: record_ttl applies to the redis backend only, and only when the "
                "backend is named explicitly (backend='redis'); it cannot be honoured for an "
                "environment-resolved backend. SQLite and Postgres expire nothing on their own, so "
                "bound record growth with record_store.purge(older_than=...) instead"
            )

    @staticmethod
    def _ttl_backend_is_redis(backend: str | None) -> bool:
        """Whether ``record_ttl`` can actually take effect.

        Only a positive identification counts. An environment-resolved backend
        is unknown at this point, so it is treated as "not Redis" rather than
        assumed compatible.
        """
        return backend is not None and backend.strip().lower() == "redis"

    def check_encryption_eagerly(self, *, backend: str | None, keyring: KeyRing | None) -> None:
        """Refuse plaintext payload capture at construction where that is knowable.

        The lifespan check is the authoritative one, since only it sees the real
        store, but raising there means the server fails during startup rather
        than at the call that misconfigured it. Whenever the keyring is already
        known (a caller-supplied record store, or an explicit ``backend=``), say
        so immediately instead.
        """
        from mcp_persist.records import PLAINTEXT_PAYLOAD_ERROR, require_payload_encryption

        if not self.enabled or self.policy is None or self.policy.is_off or self.allow_plaintext:
            return
        if self.store is not None:
            require_payload_encryption(self.store, self.policy)
        elif backend is not None and keyring is None:
            raise ValueError(f"with_persistence: {PLAINTEXT_PAYLOAD_ERROR}")


@contextlib.asynccontextmanager
async def _run_recording(
    app: Starlette,
    mcp: MCPServer,
    store: EventStore,
    options: _RecordOptions,
    warn_on_bypass: bool,
) -> AsyncIterator[None]:
    """Install the recording middleware and own the flusher's lifetime.

    The flusher is started here, in the lifespan, and never from inside a
    request: the modern transport cancels a request's whole task group on client
    disconnect, and a writer parented there would be cancelled mid-drain.

    With recording off this still installs a warning-only middleware, because
    the modern-transport bypass is worth one log line whether or not anything is
    being recorded. ``warn_on_bypass=False`` opts out of even that.
    """
    from mcp_persist.middleware import PersistenceRecorder, install, uninstall
    from mcp_persist.recorder import RecordFlusher
    from mcp_persist.records import DEFAULT_RECORD_TABLE, record_store_for, require_payload_encryption

    if not options.enabled:
        if not warn_on_bypass:
            app.state.record_store = None
            app.state.record_flusher = None
            yield
            return
        recorder = PersistenceRecorder(None, warn_on_bypass=True)
        install(mcp, recorder)
        app.state.record_store = None
        app.state.record_flusher = None
        try:
            yield
        finally:
            uninstall(mcp, recorder)
        return

    policy = options.policy or PayloadPolicy.off()
    record_store = options.store or record_store_for(
        store, table_name=options.table_name or DEFAULT_RECORD_TABLE, ttl=options.ttl
    )
    require_payload_encryption(options.store or store, policy, allow_plaintext=options.allow_plaintext)

    flusher = RecordFlusher(record_store, max_queue=options.max_queue, metrics=options.metrics)
    recorder = PersistenceRecorder(flusher, policy=policy, warn_on_bypass=warn_on_bypass)
    install(mcp, recorder)
    app.state.record_store = record_store
    app.state.record_flusher = flusher
    try:
        async with flusher:
            yield
    finally:
        # An app can be started more than once. Leaving this run's recorder
        # behind would point the next cycle at a closed writer whose queue
        # nothing drains.
        uninstall(mcp, recorder)


@contextlib.asynccontextmanager
async def _run_manager(
    app: Starlette,
    mcp: MCPServer,
    store: EventStore,
    session_idle_timeout: float | None,
    sessions: _SessionOptions,
) -> AsyncIterator[None]:
    """Run a session manager bound to ``store`` and publish it on ``app.state``.

    The mounted route (:func:`_handle_mcp`) reads ``app.state.session_manager``
    rather than closing over the manager, because the manager must be built
    inside the lifespan once the store is open. ``app.state.event_store`` is
    exposed too so callers can reach the live store (e.g. to run a
    :class:`~mcp_persist.PurgeScheduler` alongside the server), and
    ``app.state.session_registry`` when durable sessions are on.
    """
    kwargs: dict[str, Any] = {}
    if session_idle_timeout is not None:
        kwargs["session_idle_timeout"] = session_idle_timeout

    manager: StreamableHTTPSessionManager
    registry = None
    if sessions.durable:
        from mcp_persist.session_manager import ResumableSessionManager
        from mcp_persist.sessions import DEFAULT_SESSION_TABLE, session_registry_for

        registry = session_registry_for(store, table_name=sessions.table_name or DEFAULT_SESSION_TABLE)
        await registry.initialize()
        manager = ResumableSessionManager(app=mcp._lowlevel_server, event_store=store, registry=registry, **kwargs)
    else:
        manager = SessionScopedSessionManager(app=mcp._lowlevel_server, event_store=store, **kwargs)

    app.state.session_manager = manager
    app.state.event_store = store
    app.state.session_registry = registry
    async with manager.run():
        yield


async def _handle_mcp(scope: Any, receive: Any, send: Any) -> None:
    await scope["app"].state.session_manager.handle_request(scope, receive, send)


def _resolve_store(
    store: EventStore | None,
    *,
    backend: str | None,
    url: str | None,
    ttl: int | None,
    table_name: str | None,
    key_prefix: str | None,
    max_stream_length: int | None,
    tenant_id: str | None,
    compression: str | None,
    compress_min_bytes: int,
    keyring: KeyRing | None,
    batch_max_events: int | None,
    batch_max_latency_ms: float | None,
) -> tuple[AbstractAsyncContextManager[EventStore] | None, EventStore | None]:
    """Resolve the configuration into ``(ctx, store)`` with exactly one non-None.

    ``ctx`` is a store-building context manager we own (and must enter/exit);
    ``store`` is a caller-owned store we must not close.
    """
    if store is not None:
        supplied = _names_set(
            backend=backend,
            url=url,
            ttl=ttl,
            table_name=table_name,
            key_prefix=key_prefix,
            max_stream_length=max_stream_length,
            tenant_id=tenant_id,
            compression=compression,
            keyring=keyring,
            batch_max_events=batch_max_events,
            batch_max_latency_ms=batch_max_latency_ms,
        )
        if compress_min_bytes != 1024:
            supplied.append("compress_min_bytes")
        if supplied:
            raise ValueError(
                "with_persistence: pass either store= or backend=/url= with configuration options, not both"
            )
        return None, store

    if backend is not None:
        return _build_store_ctx(
            backend,
            url,
            ttl=ttl,
            table_name=table_name,
            key_prefix=key_prefix,
            max_stream_length=max_stream_length,
            tenant_id=tenant_id,
            compression=compression,
            compress_min_bytes=compress_min_bytes,
            keyring=keyring,
            batch_max_events=batch_max_events,
            batch_max_latency_ms=batch_max_latency_ms,
        ), None

    # Neither store nor backend: configuration comes from MCP_PERSIST_* env vars.
    # Reject config kwargs here so they don't get silently ignored.
    stray = _names_set(
        url=url,
        ttl=ttl,
        table_name=table_name,
        key_prefix=key_prefix,
        max_stream_length=max_stream_length,
        tenant_id=tenant_id,
        compression=compression,
        keyring=keyring,
        batch_max_events=batch_max_events,
        batch_max_latency_ms=batch_max_latency_ms,
    )
    if compress_min_bytes != 1024:
        stray.append("compress_min_bytes")
    if stray:
        raise ValueError(
            f"with_persistence: {', '.join(stray)} require backend=; with neither store= nor backend= "
            "set, the store is configured from MCP_PERSIST_* environment variables"
        )
    return event_store_from_env(), None


def _build_store_ctx(
    backend: str,
    url: str | None,
    *,
    ttl: int | None,
    table_name: str | None,
    key_prefix: str | None,
    max_stream_length: int | None,
    tenant_id: str | None,
    compression: str | None,
    compress_min_bytes: int,
    keyring: KeyRing | None,
    batch_max_events: int | None,
    batch_max_latency_ms: float | None,
) -> AbstractAsyncContextManager[EventStore]:
    if not url:
        raise ValueError("with_persistence: backend= requires url=")
    name = backend.strip().lower()

    if name not in _BACKENDS:
        raise ValueError(f"with_persistence: backend must be one of {_BACKENDS}, got {backend!r}")
    if name == "sqlite":
        _reject(name, key_prefix=key_prefix, max_stream_length=max_stream_length)
    elif name == "redis":
        _reject(name, table_name=table_name)
    else:
        _reject(name, key_prefix=key_prefix, max_stream_length=max_stream_length)

    return build_store_context(
        name,
        url,
        ttl=ttl,
        table_name=table_name,
        key_prefix=key_prefix,
        max_stream_length=max_stream_length,
        tenant_id=tenant_id,
        compression=compression,
        compress_min_bytes=compress_min_bytes,
        keyring=keyring,
        batch_max_events=batch_max_events,
        batch_max_latency_ms=batch_max_latency_ms,
    )


def _reject(backend: str, **inapplicable: Any) -> None:
    bad = _names_set(**inapplicable)
    if bad:
        raise ValueError(f"with_persistence: {', '.join(bad)} not supported by backend {backend!r}")


def _names_set(**kwargs: Any) -> list[str]:
    return sorted(name for name, value in kwargs.items() if value is not None)
