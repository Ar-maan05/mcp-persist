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

import logging
from typing import TYPE_CHECKING, Any, cast

import anyio
from mcp.server.auth.middleware.bearer_auth import (
    AuthenticatedUser,
    AuthorizationContext,
    authorization_context,
)
from mcp.server.runner import serve_loop
from mcp.server.streamable_http import MCP_SESSION_ID_HEADER, StreamableHTTPServerTransport
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.requests import Request

from mcp_persist.sessions import _owner_matches

if TYPE_CHECKING:
    from anyio.abc import TaskStatus
    from starlette.types import Receive, Scope, Send

    from mcp_persist.sessions import SessionRegistry

logger = logging.getLogger(__name__)


class ResumableSessionManager(StreamableHTTPSessionManager):
    """A ``StreamableHTTPSessionManager`` backed by a durable session registry.

    Every session this manager creates is recorded in the registry, and every
    session id it does not recognize is looked up there before being rejected.

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
        await super()._handle_stateful_request(scope, receive, send)
        await self._reconcile(known_before, session_id, requestor)

    async def _reconcile(
        self,
        known_before: set[str],
        session_id: str | None,
        requestor: dict[str, Any] | None,
    ) -> None:
        """Record what the upstream handler just did.

        Rather than reimplement the SDK's session creation to learn the new id,
        diff the manager's own instance map across the call. That keeps this
        working if the creation path changes shape upstream.
        """
        for new_id in set(self._server_instances) - known_before:
            await self._registry.register(new_id, owner=requestor)
            self._hook_termination(self._server_instances[new_id], new_id)

        if session_id is None:
            return
        transport = self._server_instances.get(session_id)
        if transport is None:
            # Gone after handling: an explicit DELETE is the usual reason.
            await self._registry.terminate(session_id)
        elif transport.is_terminated:  # pragma: no cover - terminate() hook normally wins
            await self._registry.terminate(session_id)
        else:
            await self._registry.touch(session_id)

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
        registry = self._registry

        async def terminate_and_record() -> None:
            try:
                await original()
            finally:
                try:
                    await registry.terminate(session_id)
                except Exception:  # pragma: no cover - never break teardown
                    logger.exception("Failed to record termination of session %s", session_id[:64])

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
            if session_id in self._server_instances:
                await self._server_instances[session_id].handle_request(scope, receive, send)
                return True

            transport = StreamableHTTPServerTransport(
                mcp_session_id=session_id,
                is_json_response_enabled=self.json_response,
                event_store=self.event_store,
                security_settings=self.security_settings,
                retry_interval=self.retry_interval,
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
                        idle_scope = anyio.CancelScope()
                        if self.session_idle_timeout is not None:
                            idle_scope.deadline = anyio.current_time() + self.session_idle_timeout
                            transport.idle_scope = idle_scope
                        with idle_scope:
                            await serve_loop(
                                self.app,
                                read_stream,
                                write_stream,
                                lifespan_state=self._lifespan_state,
                                session_id=session_id,
                            )
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
            await self._registry.touch(session_id)
            await transport.handle_request(scope, receive, send)
            return True


def _requestor_of(scope: Scope) -> dict[str, Any] | None:
    """The authorization context for this request, or None when unauthenticated.

    Mirrors what the SDK derives in-process, so the value stored in the registry
    is comparable to the one the SDK compares against.
    """
    user = scope.get("user")
    if not isinstance(user, AuthenticatedUser):
        return None
    return dict(authorization_context(user))
