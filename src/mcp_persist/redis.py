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
from typing import Any

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventMessage,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage
from pydantic import TypeAdapter

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
    """

    def __init__(
        self,
        redis: Any,  # redis.asyncio.Redis at runtime
        *,
        key_prefix: str = "mcp:",
        ttl: int | None = None,
    ) -> None:
        self._redis = redis
        self._prefix = key_prefix
        self._ttl = ttl

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

        # Write the event hash, its sorted-set entry, and their TTLs atomically
        # in a single round-trip, so a mid-write crash can't leave an orphaned
        # hash or a key without its expiry. The counter (incremented above) is
        # deliberately never expired: tying its lifetime to ttl would restart
        # EventIds from 1 after an idle gap longer than ttl, breaking the
        # monotonic-ID guarantee — the same reason SQLite/Postgres keep their
        # AUTOINCREMENT/IDENTITY sequence for the life of the table.
        async with self._redis.pipeline(transaction=True) as pipe:
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

        stream_id_raw: bytes | None = await self._redis.hget(self._event_key(last_event_id), "stream_id")

        if stream_id_raw is None:
            return None

        stream_id: StreamId = stream_id_raw.decode("utf-8")

        raw_ids: list[bytes] = await self._redis.zrangebyscore(
            self._stream_key(stream_id),
            min=last_int + 1,
            max="+inf",
        )

        for eid_bytes in raw_ids:
            eid: EventId = eid_bytes.decode("utf-8")

            payload_raw: bytes | None = await self._redis.hget(self._event_key(eid), "payload")

            if payload_raw is None:
                logger.debug("Event %s payload missing during replay (expired?)", eid)
                continue

            payload_str = payload_raw.decode("utf-8")

            if not payload_str:
                continue

            message = jsonrpc_message_adapter.validate_json(payload_str)

            await send_callback(EventMessage(message=message, event_id=eid))

        return stream_id
