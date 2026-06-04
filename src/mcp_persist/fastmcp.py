"""FastMCP plugin for mcp-persist.

Wire SSE stream resumability into a :class:`~mcp.server.fastmcp.FastMCP` server
with a single call. :func:`with_persistence` takes the ``FastMCP`` instance and
returns a runnable Starlette ASGI app with a
:class:`~mcp.server.streamable_http_manager.StreamableHTTPSessionManager`
already wired to an :class:`~mcp.server.streamable_http.EventStore`, managing the
store and manager lifecycle for you via the app's lifespan.

Three ways to supply the store, in resolution order:

Pattern A — config kwargs (most common)::

    from mcp.server.fastmcp import FastMCP
    from mcp_persist import with_persistence

    mcp = FastMCP(name="MyServer")
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
from typing import TYPE_CHECKING, Any

from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.applications import Starlette
from starlette.routing import Mount

from mcp_persist.config import event_store_from_env
from mcp_persist.postgres import PostgresEventStore
from mcp_persist.redis import RedisEventStore
from mcp_persist.sqlite import SQLiteEventStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AbstractAsyncContextManager

    from mcp.server.fastmcp import FastMCP
    from mcp.server.streamable_http import EventStore

_BACKENDS = ("sqlite", "redis", "postgres")


def with_persistence(
    mcp: FastMCP,
    store: EventStore | None = None,
    *,
    backend: str | None = None,
    url: str | None = None,
    ttl: int | None = None,
    table_name: str | None = None,  # sqlite / postgres
    key_prefix: str | None = None,  # redis
    max_stream_length: int | None = None,  # redis
    session_idle_timeout: float | None = None,
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
       manager and closed on app shutdown. ``ttl``/``table_name`` apply to
       sqlite & postgres; ``key_prefix``/``max_stream_length`` apply to redis.
       Passing an option that does not apply to the chosen backend is an error.
    3. neither — falls back to :func:`~mcp_persist.event_store_from_env`, which
       reads ``MCP_PERSIST_*`` from the environment. In this case passing any of
       ``url``/``ttl``/``table_name``/``key_prefix``/``max_stream_length`` is an
       error, since configuration comes from the environment.

    Args:
        mcp: The ``FastMCP`` server to serve.
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
        mcp_path: Mount path for the MCP endpoint (default ``"/mcp"``).
    """
    ctx, owned_store = _resolve_store(
        store,
        backend=backend,
        url=url,
        ttl=ttl,
        table_name=table_name,
        key_prefix=key_prefix,
        max_stream_length=max_stream_length,
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        # ctx is set when we own the store's lifecycle (Patterns A & C); enter it
        # so the store is opened on startup and closed on shutdown. When a store
        # was passed in (Pattern B), ctx is None and we leave it untouched.
        if ctx is not None:
            async with ctx as resolved_store:
                async with _run_manager(app, mcp, resolved_store, session_idle_timeout):
                    yield
        else:
            assert owned_store is not None
            async with _run_manager(app, mcp, owned_store, session_idle_timeout):
                yield

    return Starlette(lifespan=lifespan, routes=[Mount(mcp_path, app=_handle_mcp)])


@contextlib.asynccontextmanager
async def _run_manager(
    app: Starlette,
    mcp: FastMCP,
    store: EventStore,
    session_idle_timeout: float | None,
) -> AsyncIterator[None]:
    """Run a session manager bound to ``store`` and publish it on ``app.state``.

    The mounted route (:func:`_handle_mcp`) reads ``app.state.session_manager``
    rather than closing over the manager, because the manager must be built
    inside the lifespan once the store is open. ``app.state.event_store`` is
    exposed too so callers can reach the live store (e.g. to run a
    :class:`~mcp_persist.PurgeScheduler` alongside the server).
    """
    kwargs: dict[str, Any] = {}
    if session_idle_timeout is not None:
        kwargs["session_idle_timeout"] = session_idle_timeout
    manager = StreamableHTTPSessionManager(app=mcp._mcp_server, event_store=store, **kwargs)
    app.state.session_manager = manager
    app.state.event_store = store
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
) -> tuple[AbstractAsyncContextManager[EventStore] | None, EventStore | None]:
    """Resolve the configuration into ``(ctx, store)`` with exactly one non-None.

    ``ctx`` is a store-building context manager we own (and must enter/exit);
    ``store`` is a caller-owned store we must not close.
    """
    if store is not None:
        if backend is not None or url is not None:
            raise ValueError("with_persistence: pass either store= or backend=/url=, not both")
        return None, store

    if backend is not None:
        return _build_store_ctx(
            backend,
            url,
            ttl=ttl,
            table_name=table_name,
            key_prefix=key_prefix,
            max_stream_length=max_stream_length,
        ), None

    # Neither store nor backend: configuration comes from MCP_PERSIST_* env vars.
    # Reject config kwargs here so they don't get silently ignored.
    stray = _names_set(
        url=url,
        ttl=ttl,
        table_name=table_name,
        key_prefix=key_prefix,
        max_stream_length=max_stream_length,
    )
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
) -> AbstractAsyncContextManager[EventStore]:
    if not url:
        raise ValueError("with_persistence: backend= requires url=")
    name = backend.strip().lower()

    if name == "sqlite":
        _reject(name, key_prefix=key_prefix, max_stream_length=max_stream_length)
        kwargs: dict[str, Any] = {"ttl": ttl}
        if table_name is not None:
            kwargs["table_name"] = table_name
        return SQLiteEventStore.create(url, **kwargs)

    if name == "redis":
        _reject(name, table_name=table_name)
        kwargs = {"ttl": ttl}
        if key_prefix is not None:
            kwargs["key_prefix"] = key_prefix
        if max_stream_length is not None:
            kwargs["max_stream_length"] = max_stream_length
        return RedisEventStore.create(url, **kwargs)

    if name == "postgres":
        _reject(name, key_prefix=key_prefix, max_stream_length=max_stream_length)
        kwargs = {"ttl": ttl}
        if table_name is not None:
            kwargs["table_name"] = table_name
        return PostgresEventStore.create(url, **kwargs)

    raise ValueError(f"with_persistence: backend must be one of {_BACKENDS}, got {backend!r}")


def _reject(backend: str, **inapplicable: Any) -> None:
    bad = _names_set(**inapplicable)
    if bad:
        raise ValueError(f"with_persistence: {', '.join(bad)} not supported by backend {backend!r}")


def _names_set(**kwargs: Any) -> list[str]:
    return sorted(name for name, value in kwargs.items() if value is not None)
