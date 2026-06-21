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
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventMessage,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage
from pydantic import TypeAdapter

from mcp_persist.compression import compress_payload, decompress_payload, validate_compression
from mcp_persist.metrics import NoOpMetricsCollector, safe_call

if TYPE_CHECKING:
    from mcp_persist.metrics import MetricsCollector

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
                    Trimming removes only the stream-index entries, not the event
                    payload hashes; pair ``max_stream_length`` with a ``ttl`` so
                    the orphaned payloads expire rather than accumulating in Redis
                    (a warning is logged if you set this with ``ttl=None``).
        metrics:    Optional :class:`~mcp_persist.metrics.MetricsCollector` for
                    timing/count hooks on ``store_event`` and
                    ``replay_events_after``. ``None`` (the default) installs a
                    no-op collector and the store takes a fast path with no
                    measurable overhead.
        enable_streaming:
                    When ``True``, ``store_event`` publishes a lightweight
                    notification (a Redis ``PUBLISH`` of the new event ID) after
                    each non-priming write so :meth:`subscribe` can deliver
                    events in real time. ``False`` (the default) means no extra
                    round-trip per write and :meth:`subscribe` raises if called.
                    The publish is best-effort: a failure is logged and never
                    fails the write.
        compression:
                    Optional payload codec. ``"gzip"`` gzip-compresses event
                    payloads above ``compress_min_bytes`` before storing them,
                    cutting Redis memory for large messages; ``None`` (the
                    default) stores them as-is. Decompression on read is automatic
                    and independent of this setting — a store reads compressed
                    payloads written by another store even with compression off,
                    so the option is safe to roll out incrementally and across
                    :func:`mcp_persist.migrate`.
        compress_min_bytes:
                    Only compress payloads whose serialized size is at least this
                    many bytes (default ``1024``). Smaller payloads are stored
                    plain, since base64 overhead would outweigh the saving.
                    Ignored when ``compression`` is ``None``.

    The Redis client may be configured with ``decode_responses`` either way:
    values are normalized whether Redis returns ``bytes`` or ``str``.
    """

    def __init__(
        self,
        redis: Any,  # redis.asyncio.Redis at runtime
        *,
        key_prefix: str = "mcp:",
        tenant_id: str | None = None,
        ttl: int | None = None,
        max_stream_length: int | None = None,
        metrics: MetricsCollector | None = None,
        enable_streaming: bool = False,
        compression: str | None = None,
        compress_min_bytes: int = 1024,
    ) -> None:
        if max_stream_length is not None and max_stream_length <= 0:
            raise ValueError(f"max_stream_length must be a positive integer or None, got {max_stream_length!r}")
        validate_compression(compression)
        if compress_min_bytes < 0:
            raise ValueError(f"compress_min_bytes must be a non-negative integer, got {compress_min_bytes!r}")

        self._redis = redis
        self._tenant_id = tenant_id
        self._prefix = f"{key_prefix}{tenant_id}:" if tenant_id else key_prefix
        self._ttl = ttl
        self._max_stream_length = max_stream_length
        self._metrics: MetricsCollector = metrics if metrics is not None else NoOpMetricsCollector()
        self._enable_streaming = enable_streaming
        self._compression = compression
        self._compress_min_bytes = compress_min_bytes

        if ttl is None:
            logger.warning(
                "RedisEventStore created with ttl=None. "
                "Events will accumulate indefinitely in Redis. "
                "Set ttl= to a positive number of seconds "
                "(recommended: at least 2× your session_idle_timeout)."
            )

        if ttl is None and max_stream_length is not None:
            logger.warning(
                "RedisEventStore created with max_stream_length set but ttl=None. "
                "Trimming the stream index to max_stream_length drops old event IDs "
                "but does not delete their payload hashes, which without a ttl never "
                "expire and accumulate in Redis indefinitely. Set ttl= so trimmed "
                "payloads expire on their own."
            )

    # Convenience constructor

    @classmethod
    @asynccontextmanager
    async def create(
        cls,
        url: str,
        *,
        key_prefix: str = "mcp:",
        ttl: int | None = None,
        max_stream_length: int | None = None,
        **connect_kwargs: Any,
    ) -> AsyncIterator[RedisEventStore]:
        """Open a Redis connection, yield a store, and close the connection on exit.

        A convenience async context manager that owns the connection lifecycle so
        callers don't have to construct and close a ``redis.asyncio`` client
        themselves::

            async with RedisEventStore.create("redis://localhost:6379", ttl=3600) as store:
                await store.store_event(...)

        ``url`` is passed to ``redis.asyncio.from_url`` along with any extra
        ``connect_kwargs`` (e.g. ``decode_responses=``, ``max_connections=``);
        ``key_prefix``, ``ttl``, and ``max_stream_length`` configure the store and
        behave exactly as in :meth:`__init__`. The client is always closed on
        exit, including when the body raises. To manage the connection yourself
        (e.g. to share one client across stores), construct the store directly.

        Requires the ``redis`` extra (``pip install "mcp-persist[redis]"``); the
        import happens here, not at module import time, so the package loads
        without ``redis`` installed.
        """
        import redis.asyncio as aioredis

        client = aioredis.from_url(url, **connect_kwargs)
        store = cls(client, key_prefix=key_prefix, ttl=ttl, max_stream_length=max_stream_length)
        try:
            yield store
        finally:
            # redis-py >= 5.0 exposes aclose(); 4.2–4.x only has close() (the
            # declared floor is redis>=4.2.0). Mirror the guard the test suite
            # already uses so create() works across the supported range.
            try:
                await client.aclose()
            except AttributeError:  # pragma: no cover - depends on installed redis-py version
                await client.close()

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

    async def ping(self) -> bool:
        """Check Redis is reachable, for readiness/health probes.

        Issues a Redis ``PING``. Returns ``True`` on success and lets any
        connection error propagate, so a probe can treat a raised exception as
        "not ready".
        """
        return bool(await self._redis.ping())

    # EventStore interface

    async def store_event(
        self,
        stream_id: StreamId,
        message: JSONRPCMessage | None,
    ) -> EventId:
        """Store an event and return its unique, monotonically increasing ID."""
        if type(self._metrics) is NoOpMetricsCollector:
            return await self._store_event_impl(stream_id, message)
        start = time.monotonic()
        try:
            event_id = await self._store_event_impl(stream_id, message)
        except Exception as exc:
            safe_call(self._metrics.on_error, "store_event", exc)
            raise
        safe_call(self._metrics.on_store_event, stream_id, event_id, (time.monotonic() - start) * 1000.0)
        return event_id

    async def _store_event_impl(
        self,
        stream_id: StreamId,
        message: JSONRPCMessage | None,
    ) -> EventId:
        event_id_int: int = await self._redis.incr(self._counter_key())
        event_id: EventId = str(event_id_int)

        if message is None:
            payload = ""
        else:
            payload = message.model_dump_json(
                by_alias=True,
                exclude_none=True,
            )
            payload = compress_payload(payload, codec=self._compression, min_bytes=self._compress_min_bytes)

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

        # Notify real-time subscribers after the event is durably written. Only
        # for real events (priming events carry no message and are not delivered
        # by subscribe). Best-effort: a publish failure must not fail the write.
        if self._enable_streaming and message is not None:
            await self._publish_notification(stream_id, event_id)

        return event_id

    async def _allocate_event_ids(self, n: int) -> list[EventId]:
        if n < 1:
            raise ValueError(f"n must be a positive integer, got {n!r}")
        end = await self._redis.incrby(self._counter_key(), n)
        start = end - n + 1
        return [str(i) for i in range(start, end + 1)]

    async def _store_event_with_id(
        self,
        stream_id: StreamId,
        message: JSONRPCMessage | None,
        event_id: EventId,
    ) -> None:
        event_id_int = int(event_id)
        if message is None:
            payload = ""
        else:
            payload = message.model_dump_json(by_alias=True, exclude_none=True)
            payload = compress_payload(payload, codec=self._compression, min_bytes=self._compress_min_bytes)

        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.hset(
                self._event_key(event_id),
                mapping={"stream_id": stream_id, "payload": payload},
            )
            pipe.zadd(self._stream_key(stream_id), {event_id: event_id_int})
            if self._max_stream_length is not None:
                pipe.zremrangebyrank(self._stream_key(stream_id), 0, -(self._max_stream_length + 1))
            if self._ttl is not None:
                pipe.expire(self._event_key(event_id), self._ttl)
                pipe.expire(self._stream_key(stream_id), self._ttl)
            await pipe.execute()

        if self._enable_streaming and message is not None:
            await self._publish_notification(stream_id, event_id)

    async def _store_event_raw(
        self,
        stream_id: StreamId,
        event_id: EventId,
        payload: str,
        created_at: float,
    ) -> None:
        """Insert an event with an explicit ``event_id`` (idempotent overwrite).

        Bumps the global counter when ``event_id`` exceeds it so later
        :meth:`store_event` calls stay monotonic. When used as a cold archive
        store, set ``ttl=None`` so archived events are not re-expired by Redis.
        """
        event_id_int = int(event_id)
        current = await self._redis.get(self._counter_key())
        if current is None or int(current) < event_id_int:
            await self._redis.set(self._counter_key(), event_id_int)

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
            if self._ttl is not None:
                pipe.expire(self._event_key(event_id), self._ttl)
                pipe.expire(self._stream_key(stream_id), self._ttl)
            await pipe.execute()

    async def _event_exists(self, event_id: EventId) -> bool:
        return await self._redis.exists(self._event_key(event_id)) > 0

    async def _stream_id_for_event(self, event_id: EventId) -> StreamId | None:
        stream_id_raw = await self._redis.hget(self._event_key(event_id), "stream_id")
        if stream_id_raw is None:
            return None
        return cast(StreamId, self._decode(stream_id_raw))

    async def replay_events_after(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> StreamId | None:
        """Replay all events on the same stream that occurred after last_event_id."""
        if type(self._metrics) is NoOpMetricsCollector:
            return await self._replay_events_after_impl(last_event_id, send_callback)
        start = time.monotonic()
        count = 0

        async def counting_callback(event: EventMessage) -> None:
            nonlocal count
            count += 1
            await send_callback(event)

        try:
            stream_id = await self._replay_events_after_impl(last_event_id, counting_callback)
        except Exception as exc:
            safe_call(self._metrics.on_error, "replay_events_after", exc)
            raise
        safe_call(self._metrics.on_replay, stream_id, count, (time.monotonic() - start) * 1000.0)
        return stream_id

    async def _replay_events_after_impl(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> StreamId | None:
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
                message = jsonrpc_message_adapter.validate_json(decompress_payload(payload_str))
            except Exception as exc:  # noqa: BLE001 - corrupt payload (bad JSON or undecompressible); skip it, don't abort the stream
                # A single corrupt/unparseable payload must not abort the whole
                # replay: a reconnecting client would otherwise lose every event
                # on the stream, not just the bad one. Skip it and keep going.
                logger.warning(
                    "Skipping event %s on stream %s during replay: failed JSONRPC validation/decompression: %s",
                    eid,
                    stream_id,
                    exc,
                )
                continue

            await send_callback(EventMessage(message=message, event_id=eid))

        if stale:
            # Their stream-index entries are still after the anchor, but the
            # payloads are gone (expired, or a mid-write disconnect never wrote
            # them), so the resuming client misses them with no other signal.
            logger.warning(
                "Replay gap on stream %s: %d event(s) after Last-Event-ID %s are missing "
                "(expired or never completed) and cannot be replayed; the resuming client will miss them.",
                stream_id,
                len(stale),
                last_event_id,
            )
            await self._redis.zrem(self._stream_key(stream_id), *stale)

        return stream_id

    # Migration support

    async def list_streams(self) -> AsyncIterator[StreamId]:
        """Yield each distinct stream ID currently stored, in arbitrary order.

        Backs :func:`mcp_persist.migrate` for whole-database migrations. Uses a
        ``SCAN`` over ``{key_prefix}stream:*`` keys, so it never blocks Redis the
        way ``KEYS`` would. Stream IDs are matched with a glob, so a stream ID
        containing a literal glob metacharacter (``*``, ``?``, ``[``) could be
        over-matched; ordinary session IDs are unaffected.
        """
        prefix = f"{self._prefix}stream:"
        async for raw_key in self._redis.scan_iter(match=f"{prefix}*"):
            key = self._decode(raw_key)
            if key is None:
                continue
            yield cast(StreamId, key.removeprefix(prefix))

    async def _iter_stream_events(self, stream_id: StreamId) -> AsyncIterator[tuple[EventId, JSONRPCMessage | None]]:
        """Yield ``(event_id, message)`` for every stored event on a stream, oldest first.

        Unlike :meth:`replay_events_after`, this enumerates the whole stream from
        the beginning (no anchor) and includes priming events (yielded as a
        ``None`` message), so :func:`mcp_persist.migrate` can copy a stream
        faithfully. Events whose payload has expired are skipped; a payload that
        fails JSONRPC validation is logged and skipped rather than aborting the
        iteration.
        """
        raw_ids = await self._redis.zrange(self._stream_key(stream_id), 0, -1)
        if not raw_ids:
            return

        event_ids: list[EventId] = [cast(EventId, self._decode(r)) for r in raw_ids]

        async with self._redis.pipeline(transaction=False) as pipe:
            for eid in event_ids:
                pipe.hget(self._event_key(eid), "payload")
            payloads = await pipe.execute()

        for eid, payload_raw in zip(event_ids, payloads):
            if payload_raw is None:
                # Payload hash expired but the ID lingered in the index; nothing
                # to migrate.
                continue

            payload_str = self._decode(payload_raw)

            if not payload_str:
                # Priming event: stored with an empty payload, copied as None.
                yield eid, None
                continue

            try:
                message = jsonrpc_message_adapter.validate_json(decompress_payload(payload_str))
            except Exception as exc:  # noqa: BLE001 - corrupt payload (bad JSON or undecompressible); skip it, don't abort the stream
                logger.warning(
                    "Skipping event %s on stream %s during migration: failed JSONRPC validation/decompression: %s",
                    eid,
                    stream_id,
                    exc,
                )
                continue

            yield eid, message

    # Push-based streaming

    def _notify_channel(self, stream_id: StreamId) -> str:
        return f"{self._prefix}notify:{stream_id}"

    async def _publish_notification(self, stream_id: StreamId, event_id: EventId) -> None:
        """Publish a new event ID to the stream's notify channel (best-effort)."""
        try:
            await self._redis.publish(self._notify_channel(stream_id), event_id)
        except Exception:  # noqa: BLE001 - notification is best-effort; never fail the write
            logger.warning(
                "Failed to publish streaming notification for event %s on stream %s",
                event_id,
                stream_id,
                exc_info=True,
            )

    async def subscribe(self, stream_id: StreamId) -> AsyncIterator[tuple[EventId, JSONRPCMessage]]:
        """Yield ``(event_id, message)`` for events on a stream in real time.

        Requires ``enable_streaming=True``. Subscribes to the stream's Redis
        pub/sub channel and yields each new event as it is written::

            async for event_id, message in store.subscribe("stream-abc"):
                ...

        **Forward-only and best-effort (at-most-once).** Only events written
        *after* the subscription is established are delivered; use
        :meth:`replay_events_after` to catch up on history. Redis pub/sub does
        not buffer, so events published while no subscriber is connected (or
        during a reconnect) are missed — :meth:`replay_events_after` remains the
        durable path. Priming events and payloads that fail JSONRPC validation
        are skipped.

        The subscription uses its own dedicated connection (``client.pubsub()``),
        which is drawn from the client's connection pool — size the pool for the
        number of concurrent subscribers (see the connection-pool-sizing section
        in ``docs/production.md``). The generator is cancellable: breaking out of
        the ``async for`` (or cancelling the task) releases the connection.
        """
        if not self._enable_streaming:
            raise RuntimeError("subscribe() requires the store to be constructed with enable_streaming=True")

        channel = self._notify_channel(stream_id)
        pubsub = self._redis.pubsub()
        try:
            await pubsub.subscribe(channel)
            async for raw in pubsub.listen():
                if raw is None or raw.get("type") != "message":
                    # Skip the initial subscribe-confirmation and any non-data frames.
                    continue

                event_id = self._decode(raw["data"])
                if event_id is None:
                    continue

                payload_raw = await self._redis.hget(self._event_key(event_id), "payload")
                if payload_raw is None:
                    # Event expired between notification and fetch; nothing to deliver.
                    continue

                payload_str = self._decode(payload_raw)
                if not payload_str:
                    # Priming event; not delivered to subscribers.
                    continue

                try:
                    message = jsonrpc_message_adapter.validate_json(decompress_payload(payload_str))
                except Exception as exc:  # noqa: BLE001 - corrupt payload (bad JSON or undecompressible); skip it, don't abort the stream
                    logger.warning(
                        "Skipping event %s on stream %s during subscribe: failed JSONRPC validation/decompression: %s",
                        event_id,
                        stream_id,
                        exc,
                    )
                    continue

                yield event_id, message
        finally:
            # Close the pubsub connection unconditionally. unsubscribe is wrapped
            # in its own try, and the close lives in that try's finally, so the
            # connection is released even if unsubscribe raises — including
            # CancelledError (a BaseException, not caught by `except Exception`),
            # which is exactly what arrives when an SSE client disconnects.
            try:
                await pubsub.unsubscribe(channel)
            except Exception:  # noqa: BLE001 - best-effort; the connection must still be closed
                logger.debug("pubsub unsubscribe failed during subscribe teardown", exc_info=True)
            finally:
                try:
                    await pubsub.aclose()
                except AttributeError:  # pragma: no cover - redis-py < 5.0
                    await pubsub.close()
                except Exception:  # noqa: BLE001 - best-effort cleanup
                    logger.debug("pubsub close failed during subscribe teardown", exc_info=True)
