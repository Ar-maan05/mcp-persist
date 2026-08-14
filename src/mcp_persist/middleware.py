"""The one module that touches the SDK's middleware surface.

Everything mcp-persist knows about ``ServerMiddleware``,
``ServerRequestContext``, protocol-era names and the SDK's error types lives
here, and nothing else in the package imports them. That is deliberate:
``MCPServer.middleware`` is documented as *"Provisional - the signature is
expected to change before v2 is final"*, so the blast radius of that change has
to be one file with its own compatibility tests. The record model, the stores,
the flusher, the dashboard and the CLI all stay SDK-free and keep working
through the proxy carrier even if this module has to be rewritten.

The middleware does two jobs.

**Recording.** ``Server.middleware`` runs on every protocol era, including
``2026-07-28`` where the event store is never consulted at all, which is what
makes a record the cross-version half of this library. Structure follows the
SDK's own ``OpenTelemetryMiddleware``: wrap ``call_next``, separate ``MCPError``
from ``ValidationError`` from anything else, and treat a ``tools/call`` that
returned ``isError`` as a tool failure rather than a transport failure.

**The honest boundary.** On the modern transport a request never reaches the
session path, so a configured event store silently does nothing. That has been
true and unannounced since the SDK shipped era routing; this warns once, so an
operator finds out from a log line rather than from an empty database.

Nothing here ever awaits: recording is a non-blocking submit after the handler
returns, the original exception or cancellation is always re-raised untouched,
and no server-to-client request is ever sent (doing so while ``initialize`` is
handled inline would deadlock the connection).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import TYPE_CHECKING, Any

from anyio import get_cancelled_exc_class
from mcp.server.context import ServerMiddleware
from mcp.shared.exceptions import MCPError
from mcp_types import INVALID_PARAMS, CallToolResult
from mcp_types.version import HANDSHAKE_PROTOCOL_VERSIONS, MODERN_PROTOCOL_VERSIONS
from pydantic import ValidationError

from mcp_persist.records import PayloadPolicy, Record

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mcp.server.context import CallNext, HandlerResult, ServerRequestContext

    from mcp_persist.recorder import RecordFlusher

logger = logging.getLogger(__name__)

_BYPASS_WARNING = (
    "MCP protocol %s is a stateless single-exchange transport: it carries no session id and no "
    "resumable stream, so the configured event store is not consulted for these requests and SSE "
    "replay and durable sessions do not apply to them. This is expected, not a misconfiguration. "
    "Records are still written on every protocol version. See the support matrix in the README."
)


def install(mcp: Any, recorder: PersistenceRecorder) -> None:
    """Register ``recorder`` on ``mcp``'s middleware chain.

    The registration call lives here rather than at the call site so that
    ``MCPServer.middleware`` (documented upstream as provisional) is touched in
    exactly one module. If that surface moves, this function moves with it and
    nothing else changes.
    """
    mcp.middleware.append(recorder)


def uninstall(mcp: Any, recorder: PersistenceRecorder) -> None:
    """Remove ``recorder`` from ``mcp``'s middleware chain, if still present.

    A Starlette app can be started and shut down more than once (tests do it
    constantly, and so does any embedding that restarts a lifespan). Without
    this, each cycle would leave the previous run's recorder in the chain,
    pointing at a closed writer that accepts records into a queue nothing
    drains: silent loss, growing with every restart.
    """
    with contextlib.suppress(ValueError):
        mcp.middleware.remove(recorder)


def protocol_support() -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return ``(handshake_versions, modern_versions)`` from the installed SDK.

    Exposed so callers such as the admin CLI can describe the era boundary
    without importing SDK internals of their own.
    """
    return tuple(HANDSHAKE_PROTOCOL_VERSIONS), tuple(MODERN_PROTOCOL_VERSIONS)


def is_modern_era(protocol_version: str) -> bool:
    """Whether ``protocol_version`` uses the stateless single-exchange transport.

    Anything outside the handshake-era set routes to the modern path, which is
    exactly the test the SDK's own session manager applies before deciding
    whether a request ever reaches the event store. An unrecognized future
    version counts as modern, matching the SDK, so a new revision degrades to
    "records only" rather than to a false promise of resumability.
    """
    return protocol_version not in HANDSHAKE_PROTOCOL_VERSIONS


MAX_IDENTIFIER_BYTES = 512
"""Cap on the client-influenced identifier strings a record carries.

``method``, ``tool_name`` and the JSON-RPC ``request_id`` are chosen by the
client, not by a :class:`~mcp_persist.PayloadPolicy`, so they are recorded
whatever the policy says. They are identifiers rather than payload and are
needed to make a record mean anything, but a client is free to put arbitrary
text in a request id, so they are clipped rather than stored unbounded. See the
"What is always recorded" section of ``docs/records.md``.
"""


def _clip(value: str) -> str:
    """Bound a client-influenced identifier before it reaches the store."""
    raw = value.encode("utf-8")
    if len(raw) <= MAX_IDENTIFIER_BYTES:
        return value
    return raw[:MAX_IDENTIFIER_BYTES].decode("utf-8", "ignore")


def _clip_opt(value: str | None) -> str | None:
    """:func:`_clip` for a field that may legitimately be absent."""
    return None if value is None else _clip(value)


def _tool_name(params: Mapping[str, Any] | None) -> str | None:
    name = params.get("name") if params else None
    return name if isinstance(name, str) else None


def _is_tool_error(result: object) -> bool:
    """Whether a ``tools/call`` result reached the wire as a tool error.

    Mirrors the SDK's own detection: the model, or the camelCase alias on a raw
    dict. A tool failing is a normal outcome of a successful request, so it must
    stay distinguishable from a transport failure.
    """
    match result:
        case CallToolResult(is_error=True) | {"isError": True}:
            return True
        case _:
            return False


def _error_code(exc: MCPError) -> int | None:
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code
    error = getattr(exc, "error", None)
    code = getattr(error, "code", None)
    return code if isinstance(code, int) else None


class PersistenceRecorder(ServerMiddleware[Any]):
    """Record what the server handled, on every protocol era.

    Args:
        flusher: Where records go. None installs the middleware in
            warning-only mode, which is how ``with_persistence`` surfaces the
            modern-transport boundary when recording is off.
        policy: Which params, if any, may be captured. Defaults to capturing
            nothing.
        warn_on_bypass: Log once, per protocol version, when a request takes the
            modern path and therefore never reaches the event store.
        event_store_configured: Whether an event store is actually in play. The
            bypass warning is meaningless without one.
    """

    def __init__(
        self,
        flusher: RecordFlusher | None = None,
        *,
        policy: PayloadPolicy | None = None,
        warn_on_bypass: bool = True,
        event_store_configured: bool = True,
    ) -> None:
        self._flusher = flusher
        self._policy = policy or PayloadPolicy.off()
        self._warn_on_bypass = warn_on_bypass and event_store_configured
        self._warned: set[str] = set()

    async def __call__(self, ctx: ServerRequestContext[Any, Any], call_next: CallNext) -> HandlerResult:
        self._maybe_warn(ctx.protocol_version)
        if self._flusher is None:
            return await call_next(ctx)

        started = time.perf_counter()
        try:
            result = await call_next(ctx)
        except MCPError as exc:
            self._submit(ctx, started, "mcp_error", error_code=_error_code(exc))
            raise
        except ValidationError:
            # The message carries the client's own input, so only the code is
            # recorded. This mirrors the sanitized wire response.
            self._submit(ctx, started, "validation_error", error_code=INVALID_PARAMS)
            raise
        except BaseException as exc:
            # Covers Exception and cancellation alike. A client disconnect
            # cancels the handler's task group on the modern transport, so a
            # cancelled record is how a dropped request becomes visible. The
            # cancellation class is asked of anyio rather than hardcoded, since
            # it differs between the asyncio and trio backends. The original
            # exception propagates untouched either way.
            self._submit(ctx, started, "cancelled" if isinstance(exc, _cancelled_class()) else "exception")
            raise
        self._submit(
            ctx,
            started,
            "tool_error" if ctx.method == "tools/call" and _is_tool_error(result) else "ok",
        )
        return result

    def _maybe_warn(self, protocol_version: str) -> None:
        """Log the bypass once per protocol version, on the request path.

        A logging handler is arbitrary user code and can raise, so this is
        suppressed: a misconfigured handler must not be able to fail a request
        that would otherwise have succeeded. It emits at most once per version
        per process, so the cost is bounded regardless of traffic.
        """
        if not self._warn_on_bypass or protocol_version in self._warned:
            return
        self._warned.add(protocol_version)
        if is_modern_era(protocol_version):
            with contextlib.suppress(Exception):
                logger.warning(_BYPASS_WARNING, protocol_version)

    def _submit(
        self,
        ctx: ServerRequestContext[Any, Any],
        started: float,
        outcome: Any,
        *,
        error_code: int | None = None,
    ) -> None:
        """Build and queue a record. Never raises, never awaits."""
        assert self._flusher is not None
        try:
            tool = _tool_name(ctx.params) if ctx.method == "tools/call" else None
            payload, truncated = self._policy.select(ctx.method, ctx.params, tool_name=tool)
            self._flusher.submit(
                Record(
                    protocol_version=_clip(ctx.protocol_version),
                    method=_clip(ctx.method),
                    kind="request" if ctx.request_id is not None else "notification",
                    outcome=outcome,
                    carrier="middleware",
                    duration_ms=(time.perf_counter() - started) * 1000.0,
                    request_id=None if ctx.request_id is None else _clip(str(ctx.request_id)),
                    tool_name=_clip_opt(tool),
                    error_code=error_code,
                    payload=payload,
                    payload_truncated=truncated,
                )
            )
        except Exception:  # pragma: no cover - defensive
            # Recording is never allowed to affect the request it observed.
            with contextlib.suppress(Exception):
                logger.exception("failed to build a record; the request is unaffected")


def _cancelled_class() -> type[BaseException]:
    """The running backend's cancellation exception, or asyncio's as a fallback.

    anyio answers this per backend (``asyncio.CancelledError`` vs
    ``trio.Cancelled``), but only from inside a running async context, so the
    fallback covers being asked outside one.
    """
    try:
        return get_cancelled_exc_class()
    except Exception:  # pragma: no cover - defensive
        return asyncio.CancelledError
