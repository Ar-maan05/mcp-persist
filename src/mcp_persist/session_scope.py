"""Keep each session's events to itself in a store every session shares.

The SDK's transport files the events it stores under the id of the JSON-RPC
request they belong to, or ``_GET_stream`` for the standalone stream. The
session id is not part of that name, and ``StreamableHTTPSessionManager`` hands
every session the same event store. So two sessions that both send a request
with id ``7`` write into one stream ``"7"``, and when either resumes it with
``Last-Event-ID``, the store replays everything in that stream after the id:
the other session's events included. Clients number their requests from 0 or
1, so this needs no attacker; an honest client resuming a stream can be handed
another session's response, carrying the request id it is waiting on. A
malicious one only has to send a request with a common id and resume from its
own event id to read what other sessions receive.

:class:`SessionScopedSessionManager` closes that. It is a drop-in
``StreamableHTTPSessionManager`` that gives each session's transport a
:class:`SessionScopedEventStore`: a view of the shared store that names every
stream ``<session_id>:<stream>`` (the same scheme
:class:`~mcp_persist.PersistenceProxy` uses) and replays a stream only to the
session that owns it.

Events stored before this was in place carry no session prefix, so a client
resuming across the upgrade from an event id recorded before it gets nothing
replayed, as it would after the event aged out.
"""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any

from mcp.server.streamable_http import EventStore
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

if TYPE_CHECKING:
    from mcp.server.streamable_http import (
        EventCallback,
        EventId,
        EventMessage,
        StreamableHTTPServerTransport,
        StreamId,
    )
    from mcp_types import JSONRPCMessage

logger = logging.getLogger(__name__)

# Between the session id and the transport's own stream name. The session id
# comes first and is chosen by the server, so no client-chosen stream name can
# make one session's prefix match another's.
SESSION_STREAM_SEPARATOR = ":"


class SessionScopedEventStore(EventStore):
    """One session's view of an event store shared by many.

    Streams are stored as ``<session_id>:<stream_id>``, and a replay is served
    only when the stream the ``Last-Event-ID`` belongs to carries this
    session's prefix. Anything else replays nothing, exactly like an unknown id.
    """

    def __init__(self, store: EventStore, session_id: str) -> None:
        self._store = store
        self._session_id = session_id
        self._prefix = f"{session_id}{SESSION_STREAM_SEPARATOR}"

    @property
    def store(self) -> EventStore:
        """The shared store this view writes to."""
        return self._store

    @property
    def session_id(self) -> str:
        """The session whose streams this view can see."""
        return self._session_id

    async def store_event(self, stream_id: StreamId, message: JSONRPCMessage | None) -> EventId:
        return await self._store.store_event(self._prefix + stream_id, message)

    async def replay_events_after(self, last_event_id: EventId, send_callback: EventCallback) -> StreamId | None:
        owner = await _owning_stream(self._store, last_event_id)
        if owner is not _UNKNOWN:
            # The store can say whose event this is up front: refuse a foreign
            # id before reading anything, and stream an owned replay straight
            # through, keeping the transport's backpressure.
            if owner is None:
                return None
            if not self._owns(owner, last_event_id):
                return None
            stream_id = await self._store.replay_events_after(last_event_id, send_callback)
            return self._unprefixed(stream_id)

        # Otherwise the store only says which stream an event id belongs to once
        # it has replayed it, so hold the replay back until ownership is known.
        # It is one stream after one id, the same amount the client would be sent.
        replayed: list[EventMessage] = []

        async def collect(event: EventMessage) -> None:
            replayed.append(event)

        stream_id = await self._store.replay_events_after(last_event_id, collect)
        if stream_id is None or not self._owns(stream_id, last_event_id):
            return None
        for event in replayed:
            await send_callback(event)
        return self._unprefixed(stream_id)

    def _owns(self, stream_id: str, last_event_id: EventId) -> bool:
        if stream_id.startswith(self._prefix):
            return True
        logger.warning(
            "Blocked a replay across sessions: Last-Event-ID %s belongs to stream %s, not session %s",
            str(last_event_id)[:64],
            stream_id[:128],
            self._session_id[:64],
        )
        return False

    def _unprefixed(self, stream_id: StreamId | None) -> StreamId | None:
        # The transport matches the stream against its own request ids.
        if stream_id is None or not stream_id.startswith(self._prefix):
            return None
        return stream_id[len(self._prefix) :]


# Returned by _owning_stream when the store cannot answer the question.
_UNKNOWN: Any = object()


async def _owning_stream(store: EventStore, event_id: EventId) -> StreamId | None:
    """The stream ``event_id`` belongs to, None if there is no such event, or ``_UNKNOWN``.

    Every backend in this package has a ``_stream_id_for_event(event_id)``
    lookup (``BatchingEventStore`` passes it through to the store it wraps). A
    store without one, or with one of a different shape, gets ``_UNKNOWN`` and
    the replay is checked after the fact instead.
    """
    lookup = getattr(store, "_stream_id_for_event", None)
    if lookup is None:
        return _UNKNOWN
    try:
        if len(inspect.signature(lookup).parameters) != 1:
            return _UNKNOWN
    except (TypeError, ValueError):
        return _UNKNOWN
    try:
        return await lookup(event_id)
    except AttributeError:
        # BatchingEventStore over a store that has no lookup of its own.
        return _UNKNOWN


class SessionScopedSessionManager(StreamableHTTPSessionManager):
    """A ``StreamableHTTPSessionManager`` that keeps sessions out of each other's events.

    Use it wherever you would use the SDK's manager with a store that more than
    one session writes to, which is any store from this package. It takes the
    same arguments. Each session's transport gets a
    :class:`SessionScopedEventStore` over ``event_store`` as the manager
    registers it, before it has stored anything.
    """

    # The session table is a property so that every table the SDK assigns, in
    # __init__ or at any later point, is wrapped. Replacing it once after
    # __init__ would leave sessions unscoped, silently, if a later SDK release
    # reassigned the attribute.
    @property
    def _server_instances(self) -> dict[str, StreamableHTTPServerTransport]:  # pyright: ignore[reportIncompatibleVariableOverride]
        return self.__dict__["_mcp_persist_server_instances"]

    @_server_instances.setter
    def _server_instances(self, table: dict[str, StreamableHTTPServerTransport]) -> None:  # pyright: ignore[reportIncompatibleVariableOverride]
        scoped = _SessionScopedTransports(self)
        for session_id, transport in table.items():
            scoped[session_id] = transport
        self.__dict__["_mcp_persist_server_instances"] = scoped


class _SessionScopedTransports(dict[str, "StreamableHTTPServerTransport"]):
    """The manager's session table, scoping each transport's store as it is added.

    Every SDK release this package supports registers a new session's transport
    here before serving its first request, and it is the one point they share,
    so this is where the store is swapped. Only item assignment is hooked: that
    is how the SDK, and :class:`~mcp_persist.session_manager.ResumableSessionManager`,
    add sessions.
    """

    def __init__(self, manager: StreamableHTTPSessionManager) -> None:
        super().__init__()
        self._manager = manager

    def __setitem__(self, session_id: str, transport: StreamableHTTPServerTransport) -> None:
        _scope_transport(transport, session_id, self._manager.event_store)
        super().__setitem__(session_id, transport)


def _scope_transport(transport: Any, session_id: str, shared: EventStore | None) -> None:
    if shared is None:
        return
    if not hasattr(transport, "_event_store"):
        # Fail closed: serving without the scope would let sessions read each
        # other's events, and nothing else would notice.
        raise RuntimeError(
            "mcp-persist cannot isolate sessions on this version of the MCP SDK: its transport "
            "no longer keeps the event store where expected. Please report this."
        )
    current = transport._event_store
    if current is None or isinstance(current, SessionScopedEventStore):
        return
    transport._event_store = SessionScopedEventStore(current, session_id)
