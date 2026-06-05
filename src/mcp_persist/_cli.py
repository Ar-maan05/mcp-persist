"""``mcp-persist-proxy`` — run a :class:`~mcp_persist.PersistenceProxy` from the shell.

Two modes:

* **Point at a running upstream** — proxy an MCP server that is already up::

      mcp-persist-proxy --upstream http://localhost:8001 \\
          --backend sqlite --url events.db [--port 8000] [--path /mcp]

* **Subprocess** — start the upstream, wait for it, then proxy it; the child is
  stopped when the proxy exits::

      mcp-persist-proxy --backend redis --url redis://localhost:6379 \\
          [--port 8000] [--upstream-port 8001] -- uvicorn my_server:app --port 8001

The store is resolved like ``PersistenceProxy.create``: ``--backend`` + ``--url``
(closed on exit), or, with neither, the ``MCP_PERSIST_*`` environment variables.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Protocol

import httpx
import uvicorn

from mcp_persist.proxy import PersistenceProxy


class _Proc(Protocol):
    """The subset of ``asyncio.subprocess.Process`` that :func:`_terminate` uses."""

    @property
    def returncode(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    async def wait(self) -> int: ...


def main() -> None:
    args, command = _parse_args(sys.argv[1:])
    if not command and not args.upstream:
        _die("provide --upstream URL or a command after --")
    try:
        asyncio.run(_run(args, command))
    except ValueError as exc:  # store misconfiguration from PersistenceProxy.create
        _die(str(exc))
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        pass


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    # Split on the first "--": everything after it is the upstream command to run.
    if "--" in argv:
        split = argv.index("--")
        proxy_argv, command = argv[:split], argv[split + 1 :]
    else:
        proxy_argv, command = argv, []

    parser = argparse.ArgumentParser(
        prog="mcp-persist-proxy",
        description="Add SSE resumability in front of an MCP server.",
    )
    parser.add_argument("--upstream", help="URL of a running upstream MCP server (mode 1)")
    parser.add_argument("--backend", choices=("sqlite", "redis", "postgres"), help="event store backend")
    parser.add_argument("--url", help="store path / URL / DSN for --backend")
    parser.add_argument("--ttl", type=int, help="event ttl in seconds")
    parser.add_argument("--host", default="0.0.0.0", help="proxy bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="proxy bind port (default: 8000)")
    parser.add_argument("--path", default="/mcp", help="MCP endpoint path (default: /mcp)")
    parser.add_argument(
        "--upstream-port",
        type=int,
        default=8001,
        help="port the subprocess upstream listens on (mode 2, default: 8001)",
    )
    return parser.parse_args(proxy_argv), command


async def _run(args: argparse.Namespace, command: list[str]) -> None:
    if command:
        upstream = f"http://127.0.0.1:{args.upstream_port}"
        proc = await asyncio.create_subprocess_exec(*command)
        try:
            await _wait_until_ready(upstream)
            await _serve(args, upstream)
        finally:
            await _terminate(proc)
    else:
        await _serve(args, args.upstream)


async def _serve(args: argparse.Namespace, upstream: str) -> None:
    async with PersistenceProxy.create(
        upstream, backend=args.backend, url=args.url, ttl=args.ttl, mcp_path=args.path
    ) as proxy:
        config = uvicorn.Config(proxy, host=args.host, port=args.port, log_level="info")
        await uvicorn.Server(config).serve()


async def _wait_until_ready(url: str, timeout: float = 10.0) -> None:
    """Poll ``url`` until it answers (any HTTP status) or ``timeout`` elapses."""
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await client.get(url, timeout=1.0)
                return
            except httpx.HTTPError:
                if asyncio.get_running_loop().time() >= deadline:
                    raise RuntimeError(f"upstream at {url} did not become ready within {timeout:.0f}s") from None
                await asyncio.sleep(0.2)


async def _terminate(proc: _Proc, timeout: float = 5.0) -> None:
    """Stop the upstream child: SIGTERM, then SIGKILL if it does not exit in time."""
    if proc.returncode is not None:
        return
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


def _die(message: str) -> None:
    print(f"mcp-persist-proxy: error: {message}", file=sys.stderr)
    raise SystemExit(2)


if __name__ == "__main__":  # pragma: no cover
    main()
