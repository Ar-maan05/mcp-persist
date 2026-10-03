"""A session manager that can resume a session it never created.

:class:`~mcp.server.streamable_http_manager.StreamableHTTPSessionManager` holds
its live sessions in an in-process dict, so a session id it does not recognize
gets a 404. That is the reason a durable event store is not by itself enough to
survive a restart: the events are still there, but the client cannot reach them,
because the id it would quote in ``Last-Event-ID`` belongs to a session the new
process has never heard of.

:class:`ResumableSessionManager` consults a
:class:`~mcp_persist.sessions.SessionRegistry` before giving up. If the id is
recorded, live, and owned by the same principal, it builds a fresh transport
bound to that same session id and lets the request proceed. The client's next
``GET`` with ``Last-Event-ID`` then replays out of the event store exactly as it
would have on the original process.

What it does not do is move server-side conversation state. A transport is a
live pair of streams; it cannot be serialized. Adoption restores the session's
*identity* and its *event history*, which is what stream resumability needs. A
tool call that was still running when the process died is gone, and the client
learns that the way it always does, by not receiving its result.

Usage is normally via ``with_persistence(..., durable_sessions=True)``; the class
is public for callers who wire the session manager themselves.
"""

from __future__ import annotations

import contextlib
import inspect
import json
import logging
from typing import TYPE_CHECKING, Any, cast

import anyio
from mcp.server.auth.middleware.bearer_auth import (
    AuthenticatedUser,
    AuthorizationContext,
    authorization_context,
)
from mcp.server.connection import Connection
from mcp.server.runner import ServerRunner, serve_connection
from mcp.server.streamable_http import MCP_SESSION_ID_HEADER, StreamableHTTPServerTransport
from mcp.shared.jsonrpc_dispatcher import JSONRPCDispatcher
from starlette.requests import Request

from mcp_persist.session_scope import SessionScopedSessionManager
from mcp_persist.sessions import _owner_matches

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from anyio.abc import TaskStatus
    from starlette.types import Message, Receive, Scope, Send

    from mcp_persist.sessions import SessionRecord, SessionRegistry

logger = logging.getLogger(__name__)

# mcp 2.2 moved the idle timeout into the transport: it takes `idle_timeout`,
# builds its own `idle_scope` in connect(), and pushes the deadline back while
# requests are in flight. Earlier SDKs left all of that to the manager.
_TRANSPORT_OWNS_IDLE_TIMEOUT = "idle_timeout" in inspect.signature(StreamableHTTPServerTransport.__init__).parameters

# The largest opening request body inspected for its `initialize` params. A real
# initialize is a few hundred bytes; anything past this is not worth buffering
# a copy of, and the session is still recorded, just without its handshake.
_MAX_HANDSHAKE_BODY_BYTES = 64 * 1024


class ResumableSessionManager(SessionScopedSessionManager):
    """A ``StreamableHTTPSessionManager`` backed by a durable session registry.

    Every session this manager creates is recorded in the registry, and every
    session id it does not recognize is looked up there before being rejected.
    Like :class:`~mcp_persist.SessionScopedSessionManager`, which it builds on,
    it keeps each session's events out of every other session's replays.

    Args:
        registry: Where sessions are recorded. Share the event store's backend
            (see :func:`~mcp_persist.sessions.session_registry_for`) so the
            sessions and the events they replay live in the same place.
        adopt_sessions: Set False to record sessions without ever adopting one.
            The registry then serves only as an inventory (for
            ``mcp-persist sessions``), and unknown ids 404 as they do upstream.
        **kwargs: Passed to ``StreamableHTTPSessionManager``.
    """

    def __init__(
        self,
        *args: Any,
        registry: SessionRegistry,
        adopt_sessions: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        if self.stateless:
            raise ValueError(
                "ResumableSessionManager requires a stateful manager: stateless mode has no session ids to resume"
            )
        self._registry = registry
        self._adopt_sessions = adopt_sessions
        # A registry written against 2.1 has no `handshake` keyword; it keeps
        # working, and its adopted sessions are served uninitialized as before.
        register_params = inspect.signature(registry.register).parameters
        self._registry_takes_handshake = "handshake" in register_params or any(
            p.kind is inspect.Parameter.VAR_KEYWORD for p in register_params.values()
        )
        # Set once the manager starts shutting down. From mcp 2.2 the SDK
        # terminates every session's transport as its task is cancelled, and a
        # restart must not be recorded as the client ending its sessions: that
        # would make the very restart durable sessions exist for unrecoverable.
        self._shutting_down = False

    @contextlib.asynccontextmanager
    async def run(self) -> AsyncIterator[None]:
        async with super().run():
            try:
                yield
            finally:
                # Runs before the SDK's own teardown cancels the session tasks.
                self._shutting_down = True

    async def _handle_stateful_request(self, scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope, receive)
        session_id = request.headers.get(MCP_SESSION_ID_HEADER)
        requestor = _requestor_of(scope)

        if self._adopt_sessions and session_id is not None and session_id not in self._server_instances:
            if await self._try_adopt(session_id, requestor, scope, receive, send):
                return
            # Not adoptable (unknown, terminated, or a different principal).
            # Fall through so the upstream implementation produces the same 404
            # it always would; we deliberately do not distinguish the cases to
            # the client, matching how the SDK hides an owner mismatch.

        known_before = set(self._server_instances)
        registered: set[str] = set()
        opening_body = bytearray()

        async def receive_and_keep_opening_body() -> Message:
            # Only a request without a session id can open one, and it has to be
            # `initialize`. Keep a copy of its body so the handshake can be
            # recorded with the session and restored by whichever process adopts
            # it later.
            message = await receive()
            if message["type"] == "http.request" and len(opening_body) <= _MAX_HANDSHAKE_BODY_BYTES:
                opening_body.extend(message.get("body", b""))
            return message

        async def send_once_registered(message: Message) -> None:
            # A new session is recorded before the response that hands the client
            # its id goes out. Recording it afterwards left a window in which a
            # client that went straight to another worker found no record and
            # got a 404.
            if message["type"] == "http.response.start" and message["status"] < 400:
                new_id = _response_session_id(message)
                if new_id is not None and new_id not in known_before and new_id not in registered:
                    registered.add(new_id)
                    await self._register(new_id, requestor, _initialize_params(opening_body))
            await send(message)

        opening = session_id is None
        try:
            await super()._handle_stateful_request(
                scope, receive_and_keep_opening_body if opening else receive, send_once_registered
            )
        finally:
            # Also when the request is cancelled or raises: the session may
            # already exist by then, and this is where its termination hook is
            # installed. Without it a session created by a request that went
            # away idled out unrecorded and stayed adoptable. Shielded so the
            # cancellation that got us here does not cut the bookkeeping short.
            with anyio.CancelScope(shield=True):
                await self._reconcile(known_before, session_id, requestor, registered, _initialize_params(opening_body))

    async def _register(
        self, session_id: str, requestor: dict[str, Any] | None, handshake: dict[str, Any] | None
    ) -> None:
        try:
            if self._registry_takes_handshake:
                await self._registry.register(session_id, owner=requestor, handshake=handshake)
            else:
                await self._registry.register(session_id, owner=requestor)
        except Exception:
            # A registry outage costs this session its durability, not its
            # response: the client still gets a working session on this worker.
            logger.exception("Failed to record session %s in the durable registry", session_id[:64])

    async def _reconcile(
        self,
        known_before: set[str],
        session_id: str | None,
        requestor: dict[str, Any] | None,
        registered: set[str],
        handshake: dict[str, Any] | None,
    ) -> None:
        """Record what the upstream handler just did.

        Rather than reimplement the SDK's session creation to learn the new id,
        diff the manager's own instance map across the call. That keeps this
        working if the creation path changes shape upstream. A new session is
        normally recorded already, as its response went out; this catches one
        whose id reached the client some other way.
        """
        for new_id in set(self._server_instances) - known_before:
            if new_id not in registered:
                await self._register(new_id, requestor, handshake)
            self._hook_termination(self._server_instances[new_id], new_id)

        # Recorded as its response started, then gone before the request ended:
        # from mcp 2.2 the SDK discards a session whose opening request is
        # cancelled or fails, before any hook here could see it terminate.
        # Left alone, the registry would hand a session that never got going to
        # the next worker to ask.
        if not self._shutting_down:
            for new_id in registered - set(self._server_instances):
                await self._record_termination(new_id)

        if session_id is None:
            return
        transport = self._server_instances.get(session_id)
        if transport is None:
            # Gone after handling: an explicit DELETE is the usual reason. Only
            # if this worker held it going in; an id it never had (declined
            # adoption, a wrong credential) just got a 404, which says nothing
            # about the session, and must not end it for its real owner.
            if session_id in known_before:
                await self._record_termination(session_id)
        elif transport.is_terminated:  # pragma: no cover - terminate() hook normally wins
            await self._record_termination(session_id)
        else:
            await self._touch(session_id)

    async def _touch(self, session_id: str) -> None:
        try:
            await self._registry.touch(session_id)
        except Exception:
            # Only the session's last-seen time is lost; the request it belongs
            # to has been, or is about to be, served either way.
            logger.exception("Failed to update session %s in the durable registry", session_id[:64])

    async def _record_termination(self, session_id: str) -> None:
        try:
            await self._registry.terminate(session_id)
        except Exception:
            logger.exception("Failed to record termination of session %s", session_id[:64])

    def _hook_termination(self, transport: Any, session_id: str) -> None:
        """Mark the registry when this transport terminates, however that happens.

        The idle-timeout and crash paths run in the manager's background task, so
        there is no request in flight to notice them. They all funnel through
        ``transport.terminate()``, so wrapping that one method covers them
        without reaching further into the SDK.
        """
        if getattr(transport, "_mcp_persist_termination_hooked", False):
            return
        original = transport.terminate

        async def terminate_and_record() -> None:
            try:
                await original()
            finally:
                if self._shutting_down:
                    # The process is going away, not the session: leave it live
                    # in the registry so the next process can adopt it.
                    return
                await self._record_termination(session_id)

        try:
            transport.terminate = terminate_and_record
            transport._mcp_persist_termination_hooked = True
        except AttributeError:  # pragma: no cover - depends on the SDK's transport class
            logger.debug("Could not hook terminate() on the transport; relying on request-time reconcile")

    async def _try_adopt(
        self,
        session_id: str,
        requestor: dict[str, Any] | None,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> bool:
        """Bring a session recorded elsewhere back to life in this process.

        Returns True if the request was handled here.
        """
        try:
            record = await self._registry.get(session_id)
        except Exception:
            # A registry outage must not turn into a 500 on every request; fall
            # back to upstream behaviour (a 404) and let the client re-init.
            logger.exception("Session registry lookup failed for %s", session_id[:64])
            return False

        if record is None or record.terminated:
            return False
        if not _owner_matches(record.owner, requestor):
            logger.warning(
                "Refusing to adopt session %s: credential does not match the one that created it",
                session_id[:64],
            )
            return False

        async with self._session_creation_lock:
            # Another request may have adopted it while we waited for the lock.
            transport = self._server_instances.get(session_id)
            adopted_here = transport is None
            if transport is None:
                transport = await self._start_adopted(session_id, requestor, record)

        # Served outside the lock. The request can be a standalone GET stream
        # that stays open for as long as the client is connected, and while the
        # lock is held no session can be created or adopted on this worker; a
        # restart, where every client reconnects at once, would stall behind
        # the first one.
        if adopted_here:
            await self._touch(session_id)
        await transport.handle_request(scope, receive, send)
        return True

    async def _start_adopted(
        self, session_id: str, requestor: dict[str, Any] | None, record: SessionRecord
    ) -> StreamableHTTPServerTransport:
        """Create the transport for an adopted session and start serving it.

        Called with ``_session_creation_lock`` held, so it must not wait on a request.
        """
        transport_kwargs: dict[str, Any] = {}
        if _TRANSPORT_OWNS_IDLE_TIMEOUT:
            transport_kwargs["idle_timeout"] = self.session_idle_timeout
        transport = StreamableHTTPServerTransport(
            mcp_session_id=session_id,
            is_json_response_enabled=self.json_response,
            event_store=self.event_store,
            security_settings=self.security_settings,
            retry_interval=self.retry_interval,
            **transport_kwargs,
        )
        if requestor is not None:
            # The registry round-trips the context as a plain dict (it has to
            # be JSON), and AuthorizationContext is a TypedDict, so this is
            # the same shape by construction.
            self._session_owners[session_id] = cast("AuthorizationContext", requestor)
        self._server_instances[session_id] = transport
        self._hook_termination(transport, session_id)
        logger.info("Adopted session %s from the durable registry", session_id[:64])

        async def run_server(*, task_status: TaskStatus[None] = anyio.TASK_STATUS_IGNORED) -> None:
            async with transport.connect() as streams:
                read_stream, write_stream = streams
                task_status.started()
                try:
                    if _TRANSPORT_OWNS_IDLE_TIMEOUT:
                        # The transport built its scope in connect() and moves
                        # the deadline itself; a fixed deadline set here would
                        # expire an active session.
                        idle_scope = transport.idle_scope or anyio.CancelScope()
                    else:
                        idle_scope = anyio.CancelScope()
                        if self.session_idle_timeout is not None:
                            idle_scope.deadline = anyio.current_time() + self.session_idle_timeout
                            transport.idle_scope = idle_scope
                    with idle_scope:
                        await self._serve_adopted(read_stream, write_stream, session_id, record.handshake)
                    if idle_scope.cancelled_caught:
                        self._server_instances.pop(session_id, None)
                        self._session_owners.pop(session_id, None)
                        await transport.terminate()
                except Exception:
                    logger.exception("Adopted session %s crashed", session_id[:64])
                finally:
                    if self._server_instances.get(session_id) is transport and not transport.is_terminated:
                        del self._server_instances[session_id]
                        self._session_owners.pop(session_id, None)

        assert self._task_group is not None
        await self._task_group.start(run_server)
        return transport

    async def _serve_adopted(
        self, read_stream: Any, write_stream: Any, session_id: str, handshake: dict[str, Any] | None
    ) -> None:
        """Serve an adopted session, with its handshake already in place.

        The same dispatcher and connection ``serve_loop`` builds, except that the
        connection starts initialized from the recorded ``initialize`` params.
        The client finished its handshake with the process that created the
        session and will not repeat it, and a connection that never saw it
        refuses every method but ``ping``.
        """
        dispatcher: JSONRPCDispatcher[Any] = JSONRPCDispatcher(
            read_stream, write_stream, inline_methods=frozenset({"initialize"})
        )
        connection = Connection.for_loop(dispatcher, session_id=session_id)
        if handshake is not None:
            try:
                client_params, protocol_version = ServerRunner._negotiate_initialize(handshake)
            except Exception:
                logger.warning(
                    "Recorded handshake for session %s is unreadable; serving it uninitialized", session_id[:64]
                )
            else:
                connection.client_params = client_params
                connection.protocol_version = protocol_version
                connection.initialized.set()
        await serve_connection(self.app, dispatcher, connection=connection, lifespan_state=self._lifespan_state)


def _initialize_params(body: bytes | bytearray) -> dict[str, Any] | None:
    """The params of the ``initialize`` request in an opening request body, if any."""
    if not body or len(body) > _MAX_HANDSHAKE_BODY_BYTES:
        return None
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    for message in payload if isinstance(payload, list) else [payload]:
        if isinstance(message, dict) and message.get("method") == "initialize":
            params = message.get("params")
            return params if isinstance(params, dict) else None
    return None


def _response_session_id(message: Message) -> str | None:
    """The ``Mcp-Session-Id`` a response start message assigns, if any."""
    header = MCP_SESSION_ID_HEADER.lower().encode("latin-1")
    for name, value in message.get("headers", ()):
        if name.lower() == header:
            return value.decode("latin-1")
    return None


def _requestor_of(scope: Scope) -> dict[str, Any] | None:
    """The authorization context for this request, or None when unauthenticated.

    Mirrors what the SDK derives in-process, so the value stored in the registry
    is comparable to the one the SDK compares against.
    """
    user = scope.get("user")
    if not isinstance(user, AuthenticatedUser):
        return None
    return dict(authorization_context(user))
