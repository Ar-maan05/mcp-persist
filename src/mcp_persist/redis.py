"""Redis-backed EventStore for MCP SSE stream resumability.

Requires the redis extra:
    pip install "mcp-persist[redis]"

Quickstart:
    import redis.asyncio as aioredis
    from mcp.server.fastmcp import FastMCP
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from mcp_persist import RedisEventStore

    mcp = FastMCP(name="MyServer")
    redis_client = aioredis.from_url("redis://localhost:6379")
    store = RedisEventStore(redis_client, ttl=3600)

    session_manager = StreamableHTTPSessionManager(
        app=mcp._mcp_server,  # the low-level Server that FastMCP wraps
        event_store=store,
    )
"""

from __future__ import annotations

import logging
from typing import Any, cast

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventMessage,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage
from pydantic import TypeAdapter, ValidationError

logger = logging.getLogger(__name__)

jsonrpc_message_adapter = TypeAdapter(JSONRPCMessage)


class RedisEventStore(EventStore):
    """EventStore backed by Redis for production multi-process deployments.

    Redis data layout:
        {prefix}counter                — STRING, atomic INCR source for EventIds (never expired)
        {prefix}event:{event_id}       — HASH, fields: stream_id + payload
        {prefix}stream:{stream_id}     — ZSET, members: event_ids, scores: int(event_id)

    Args:
        redis:      An already-connected redis.asyncio.Redis instance.
        key_prefix: Prefix for all Redis keys. Use different prefixes when
                    multiple MCP servers share one Redis instance.
                    Default: "mcp:".
        ttl:        Seconds after which keys expire automatically.
                    None means keys never expire — strongly discouraged in
                    production. Recommended: at least 2× session_idle_timeout.
        max_stream_length:
                    Optional cap on how many event IDs each stream's sorted set
                    retains. On every write the oldest IDs beyond this many are
                    trimmed (``ZREMRANGEBYRANK``), bounding memory on a busy,
                    long-lived stream even if it is never resumed from. ``None``
                    (the default) leaves the set uncapped; stale IDs whose
                    payloads have expired are still pruned lazily on replay.
                    Set this above the largest backlog a client could resume
                    from — a client more than this many events behind will only
                    replay the most recent ``max_stream_length``.

    The Redis client may be configured with ``decode_responses`` either way:
    values are normalized whether Redis returns ``bytes`` or ``str``.
    """

    def __init__(
        self,
        redis: Any,  # redis.asyncio.Redis at runtime
        *,
        key_prefix: str = "mcp:",
        ttl: int | None = None,
        max_stream_length: int | None = None,
    ) -> None:
        if max_stream_length is not None and max_stream_length <= 0:
            raise ValueError(f"max_stream_length must be a positive integer or None, got {max_stream_length!r}")

        self._redis = redis
        self._prefix = key_prefix
        self._ttl = ttl
        self._max_stream_length = max_stream_length

        if ttl is None:
            logger.warning(
                "RedisEventStore created with ttl=None. "
                "Events will accumulate indefinitely in Redis. "
                "Set ttl= to a positive number of seconds "
                "(recommended: at least 2× your session_idle_timeout)."
            )

    # Key helpers

    def _counter_key(self) -> str:
        return f"{self._prefix}counter"

    def _event_key(self, event_id: EventId) -> str:
        return f"{self._prefix}event:{event_id}"

    def _stream_key(self, stream_id: StreamId) -> str:
        return f"{self._prefix}stream:{stream_id}"

    @staticmethod
    def _decode(value: bytes | str | None) -> str | None:
        """Normalize a Redis reply to ``str`` regardless of ``decode_responses``.

        Clients created with ``decode_responses=True`` already return ``str``;
        the default bytes client returns ``bytes``. Handling both lets the store
        share whatever Redis connection the rest of the application uses.
        """
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    # EventStore interface

    async def store_event(
        self,
        stream_id: StreamId,
        message: JSONRPCMessage | None,
    ) -> EventId:
        """Store an event and return its unique, monotonically increasing ID."""
        event_id_int: int = await self._redis.incr(self._counter_key())
        event_id: EventId = str(event_id_int)

        if message is None:
            payload = ""
        else:
            payload = message.model_dump_json(
                by_alias=True,
                exclude_none=True,
            )

        # Write the event hash, its sorted-set entry, and their TTLs in a single
        # pipelined round-trip. transaction=False (no MULTI/EXEC) keeps this
        # valid on Redis Cluster: the event hash and the stream index hash to
        # different slots, which a transactional pipeline would reject with
        # CROSSSLOT. The trade-off — losing cross-key atomicity — is bounded:
        # a stream-index entry left without its payload (e.g. a mid-write
        # disconnect) is pruned lazily on the next replay, and an orphaned hash
        # expires on its own ttl. The counter (incremented above) is
        # deliberately never expired: tying its lifetime to ttl would restart
        # EventIds from 1 after an idle gap longer than ttl, breaking the
        # monotonic-ID guarantee — the same reason SQLite/Postgres keep their
        # AUTOINCREMENT/IDENTITY sequence for the life of the table.
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.hset(
                self._event_key(event_id),
                mapping={
                    "stream_id": stream_id,
                    "payload": payload,
                },
            )
            pipe.zadd(
                self._stream_key(stream_id),
                {event_id: event_id_int},
            )
            if self._max_stream_length is not None:
                # Keep only the newest max_stream_length members. Scores are the
                # (monotonic) event IDs, so the lowest ranks are the oldest.
                pipe.zremrangebyrank(self._stream_key(stream_id), 0, -(self._max_stream_length + 1))
            if self._ttl is not None:
                pipe.expire(self._event_key(event_id), self._ttl)
                pipe.expire(self._stream_key(stream_id), self._ttl)
            await pipe.execute()

        return event_id

    async def replay_events_after(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> StreamId | None:
        """Replay all events on the same stream that occurred after last_event_id."""
        # Last-Event-ID is a client-controlled header; a non-numeric value can't
        # match any stored event, so return None instead of raising on int().
        try:
            last_int = int(last_event_id)
        except (TypeError, ValueError):
            return None

        stream_id_raw = await self._redis.hget(self._event_key(last_event_id), "stream_id")

        if stream_id_raw is None:
            return None

        stream_id: StreamId = cast(StreamId, self._decode(stream_id_raw))

        raw_ids = await self._redis.zrangebyscore(
            self._stream_key(stream_id),
            min=last_int + 1,
            max="+inf",
        )

        if not raw_ids:
            return stream_id

        event_ids: list[EventId] = [cast(EventId, self._decode(r)) for r in raw_ids]

        # Fetch every payload in one pipelined round-trip rather than a blocking
        # HGET per event (a 500-event backlog was 500 sequential round-trips).
        # transaction=False keeps it cluster-safe — the hashes can live on
        # different nodes.
        async with self._redis.pipeline(transaction=False) as pipe:
            for eid in event_ids:
                pipe.hget(self._event_key(eid), "payload")
            payloads = await pipe.execute()

        stale: list[EventId] = []

        for eid, payload_raw in zip(event_ids, payloads):
            if payload_raw is None:
                # The payload hash has expired but its ID lingered in the stream
                # index; collect it so the sorted set can't grow without bound
                # on a long-lived stream.
                logger.debug("Event %s payload missing during replay (expired?)", eid)
                stale.append(eid)
                continue

            payload_str = self._decode(payload_raw)

            if not payload_str:
                continue

            try:
                message = jsonrpc_message_adapter.validate_json(payload_str)
            except ValidationError:
                # A single corrupt/unparseable payload must not abort the whole
                # replay: a reconnecting client would otherwise lose every event
                # on the stream, not just the bad one. Skip it and keep going.
                logger.warning(
                    "Skipping event %s on stream %s during replay: payload failed JSONRPC validation",
                    eid,
                    stream_id,
                )
                continue

            await send_callback(EventMessage(message=message, event_id=eid))

        if stale:
            await self._redis.zrem(self._stream_key(stream_id), *stale)

        return stream_id
