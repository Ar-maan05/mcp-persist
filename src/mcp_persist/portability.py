"""Single-stream export/import: portable JSON snapshots of one stream's events.

Powers ``mcp-persist dump <stream>`` and ``mcp-persist load`` and is usable
programmatically. The intended use is bug reports and test fixtures: dump a
stream to a self-contained JSON document, hand it to someone, and load it into a
fresh store to reproduce the exact sequence of events.

Event IDs are informational. :func:`import_stream` re-stores each event with
``store_event``, so the destination assigns fresh, monotonically increasing IDs
exactly as :func:`mcp_persist.migrate` does. A restore therefore reproduces a
stream's *content and ordering*, not its original resumability tokens: a client
holding a pre-dump ``Last-Event-ID`` cannot resume against the restored copy.

The JSON envelope is stable and versioned::

    {
      "format": "mcp-persist-dump",
      "version": 1,
      "stream_id": "abc123",
      "backend": "sqlite",          # informational; absent when unknown
      "exported_at": 1751990400.0,  # unix seconds
      "events": [
        {"event_id": "1", "message": { ... }},   # a JSON-RPC message
        {"event_id": "2", "message": null}       # a priming event (no payload)
      ]
    }
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Protocol

from mcp_types import JSONRPCMessage
from pydantic import TypeAdapter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from mcp.server.streamable_http import EventId, StreamId

    class _ExportSource(Protocol):
        def _iter_stream_events(
            self, stream_id: StreamId, /
        ) -> AsyncIterator[tuple[EventId, JSONRPCMessage | None]]: ...

    class _ImportDest(Protocol):
        async def store_event(self, stream_id: StreamId, message: JSONRPCMessage | None, /) -> EventId: ...


DUMP_FORMAT = "mcp-persist-dump"
DUMP_VERSION = 1

_message_adapter: TypeAdapter[JSONRPCMessage] = TypeAdapter(JSONRPCMessage)


async def export_stream(
    store: _ExportSource,
    stream_id: str,
    *,
    backend: str | None = None,
) -> dict[str, Any]:
    """Read every event of ``stream_id`` and return a portable dump document.

    Events are read oldest-first via the same ``_iter_stream_events`` path that
    :func:`mcp_persist.migrate` uses, so a dump is decompressed and decrypted
    plaintext regardless of how the source store persists payloads. A priming
    event (stored with no message) is represented as ``{"message": null}``.

    Args:
        store:     Any backend in this package (structural: needs
                   ``_iter_stream_events``).
        stream_id: The stream to export. An unknown stream yields an empty
                   ``events`` list rather than an error.
        backend:   Optional backend label recorded in the envelope for context;
                   it does not affect what is exported.

    Returns:
        A JSON-serialisable ``dict`` following the envelope in the module
        docstring.
    """
    events: list[dict[str, Any]] = []
    async for event_id, message in store._iter_stream_events(stream_id):
        payload = None if message is None else message.model_dump(mode="json", by_alias=True, exclude_none=True)
        events.append({"event_id": str(event_id), "message": payload})

    doc: dict[str, Any] = {
        "format": DUMP_FORMAT,
        "version": DUMP_VERSION,
        "stream_id": stream_id,
        "exported_at": time.time(),
        "events": events,
    }
    if backend is not None:
        doc["backend"] = backend
    return doc


async def import_stream(
    store: _ImportDest,
    document: dict[str, Any],
    *,
    stream_id: str | None = None,
) -> int:
    """Restore a dump document into ``store`` and return the number of events written.

    Each event is re-stored with ``store_event`` in the dump's order, so the
    destination assigns fresh IDs (see the module docstring on why IDs are not
    preserved). Restoring into a store that already holds the stream *appends*;
    it does not replace.

    Args:
        store:     Any backend in this package (structural: needs
                   ``store_event``).
        document:  A dump produced by :func:`export_stream` (or the ``dump``
                   CLI). Validated for shape and format before any write.
        stream_id: Override the stream to write into. Defaults to the
                   ``stream_id`` recorded in the document; required if the
                   document omits it.

    Raises:
        ValueError: If the document is not a recognised, supported dump, or if no
            ``stream_id`` can be determined.
    """
    _validate_document(document)

    target = stream_id if stream_id is not None else document.get("stream_id")
    if not target:
        raise ValueError("no stream_id in the dump and none supplied; pass stream_id=")

    messages: list[JSONRPCMessage | None] = []
    for index, raw in enumerate(document["events"]):
        if not isinstance(raw, dict) or "message" not in raw:
            raise ValueError(f"event {index} is malformed: expected an object with a 'message' key")
        payload = raw["message"]
        message = None if payload is None else _message_adapter.validate_python(payload)
        messages.append(message)

    # Validate and parse the complete document before mutating the destination.
    # A bad event near the end must not leave a partially restored stream.
    for message in messages:
        await store.store_event(target, message)
    return len(messages)


def _validate_document(document: object) -> None:
    """Reject anything that is not a supported mcp-persist dump.

    Fails closed with an actionable message so a mistyped path or an
    incompatible future version is caught before a single event is written,
    rather than surfacing as a confusing validation error mid-restore.
    """
    if not isinstance(document, dict):
        raise ValueError("dump must be a JSON object")
    if document.get("format") != DUMP_FORMAT:
        raise ValueError(f"not an mcp-persist dump (format={document.get('format')!r}, expected {DUMP_FORMAT!r})")
    version = document.get("version")
    if version != DUMP_VERSION:
        raise ValueError(f"unsupported dump version {version!r}; this build reads version {DUMP_VERSION}")
    if not isinstance(document.get("events"), list):
        raise ValueError("dump 'events' must be a list")
