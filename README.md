# mcp-persist

Production-grade persistence backends for the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk).

The MCP SDK ships an `EventStore` interface but only an in-memory reference implementation. `mcp-persist` provides backends for real deployments where you need durability across process restarts and multi-worker environments.

## Backends

| Backend | Extra | Use case |
|---|---|---|
| `RedisEventStore` | `redis` | Multi-process / multi-worker SSE resumability |

## Installation

```bash
pip install "mcp-persist[redis]"
```

## Quickstart

```python
import redis.asyncio as aioredis
from mcp_persist import RedisEventStore
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

redis_client = aioredis.from_url("redis://localhost:6379")
store = RedisEventStore(redis_client, ttl=3600)  # 1 hour TTL

session_manager = StreamableHTTPSessionManager(
    app=mcp_server,
    event_store=store,
)
```

## RedisEventStore

Stores MCP SSE events in Redis so clients can resume interrupted streams — even across worker restarts or load-balanced deployments.

### How it works

Redis data layout:

```
{prefix}counter                 — atomic INCR source for monotonic event IDs
{prefix}event:{event_id}        — HASH: stream_id + serialized payload
{prefix}stream:{stream_id}      — ZSET: event IDs sorted by score for O(log N) range queries
```

- **Atomic monotonic IDs** via Redis `INCR` — collision-free across concurrent workers
- **O(log N) replay** via sorted set `ZRANGEBYSCORE`
- **TTL support** — automatic key expiry to prevent unbounded memory growth
- **Multi-tenant isolation** via configurable `key_prefix`
- **Priming event handling** — sentinel empty-string payloads are stored but never replayed to clients

### Configuration

```python
RedisEventStore(
    redis,                  # redis.asyncio.Redis instance
    key_prefix="mcp:",      # isolate multiple servers on one Redis instance
    ttl=3600,               # seconds; None = never expire (not recommended)
)
```

**TTL guidance:** Set `ttl` to at least 2× your session idle timeout. If you leave it as `None`, a warning is logged and events accumulate indefinitely.

### Multi-tenant deployments

If multiple MCP servers share a Redis instance, use different prefixes:

```python
store_a = RedisEventStore(redis_client, key_prefix="server-a:")
store_b = RedisEventStore(redis_client, key_prefix="server-b:")
```

## Development

```bash
git clone https://github.com/Ar-maan05/mcp-persist
cd mcp-persist
pip install -e ".[redis,dev]"
pytest tests/
```

Tests use [fakeredis](https://github.com/cunla/fakeredis-py) — no external Redis server required.

## License

MIT
