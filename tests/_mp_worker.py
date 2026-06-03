"""Out-of-process event writer for the multi-process integration test.

Run as a standalone script (``python tests/_mp_worker.py <backend> <url> <key>
<stream> <n>``) so it is a genuinely separate OS process sharing only the
Redis/Postgres backend with the test — no in-process state, no multiprocessing
pickling. It imports only ``mcp_persist`` (installed) and the stdlib, so it is
safe to launch with the same interpreter via ``subprocess``.

The leading underscore keeps pytest from collecting it as a test module.
"""

from __future__ import annotations

import asyncio

from mcp.types import JSONRPCRequest


def _msg(i: int) -> JSONRPCRequest:
    return JSONRPCRequest(jsonrpc="2.0", id=str(i), method="mp")


async def _write_redis(url: str, key_prefix: str, stream: str, n: int) -> None:
    from mcp_persist import RedisEventStore

    async with RedisEventStore.create(url, key_prefix=key_prefix, ttl=3600) as store:
        for i in range(n):
            await store.store_event(stream, _msg(i))
            await asyncio.sleep(0.005)


async def _write_postgres(dsn: str, table: str, stream: str, n: int) -> None:
    from mcp_persist import PostgresEventStore

    async with PostgresEventStore.create(dsn, table_name=table, ttl=3600) as store:
        for i in range(n):
            await store.store_event(stream, _msg(i))
            await asyncio.sleep(0.005)


def run_writer(backend: str, url: str, key: str, stream: str, n: int) -> None:
    if backend == "redis":
        asyncio.run(_write_redis(url, key, stream, n))
    elif backend == "postgres":
        asyncio.run(_write_postgres(url, key, stream, n))
    else:
        raise ValueError(f"unknown backend {backend!r}")


if __name__ == "__main__":
    import sys

    _, backend_arg, url_arg, key_arg, stream_arg, n_arg = sys.argv
    run_writer(backend_arg, url_arg, key_arg, stream_arg, int(n_arg))
