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

Pass ``--check`` to probe the upstream and exit without starting the proxy: it
verifies the upstream is reachable and answers the MCP endpoint with a Streamable
HTTP response, a quick pre-flight before committing to a long-running proxy.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Protocol

import httpx
import uvicorn

from mcp_persist.proxy import PersistenceProxy

# A minimal MCP ``initialize`` request used by ``--check`` to confirm the upstream
# speaks Streamable HTTP. The id/clientInfo are cosmetic; only the shape of the
# response (JSON-RPC body or an SSE content-type) is inspected.
_CHECK_INIT_REQUEST = {
    "jsonrpc": "2.0",
    "id": "mcp-persist-proxy-check",
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "mcp-persist-proxy", "version": "check"},
    },
}


class _Proc(Protocol):
    """The subset of ``asyncio.subprocess.Process`` that :func:`_terminate` uses."""

    @property
    def returncode(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    async def wait(self) -> int: ...


def main() -> None:
    args, command = _parse_args(sys.argv[1:])
    if args.check:
        # A pre-flight probe of a running upstream; meaningless before a
        # subprocess upstream has even started, so it requires mode 1.
        if not args.upstream:
            _die("--check requires --upstream")
        ok = asyncio.run(_check_upstream(args.upstream, args.path))
        raise SystemExit(0 if ok else 1)
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
    parser.add_argument(
        "--check",
        action="store_true",
        help="probe --upstream and exit (verify reachability + Streamable HTTP) without starting the proxy",
    )
    parser.add_argument(
        "--cors",
        nargs="?",
        const="*",
        default=None,
        metavar="ORIGIN",
        help="enable CORS for browser clients: answer preflights and add "
        "Access-Control-Allow-Origin to responses (origin defaults to *)",
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
        upstream, backend=args.backend, url=args.url, ttl=args.ttl, mcp_path=args.path, cors=args.cors
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


def _print_check(level: str, name: str, detail: str) -> None:
    tag = {"ok": "[ ok ]", "warn": "[warn]", "fail": "[fail]"}[level]
    print(f"{tag} {name:<16}{detail}")


async def _check_upstream(
    upstream: str,
    mcp_path: str,
    *,
    client: httpx.AsyncClient | None = None,
    timeout: float = 5.0,
) -> bool:
    """Probe ``upstream`` and report whether it is ready to proxy.

    Two honest levels, printed as a pass/fail checklist:

    * **reachable**: an HTTP connection to the MCP endpoint succeeds at all. A
      connection error fails here and stops (nothing else is knowable).
    * **streamable-http**: a minimal MCP ``initialize`` POST returns something
      that looks like Streamable HTTP: a ``text/event-stream`` response, or a
      JSON-RPC body. A 404/405 means the path is wrong (a ``fail``, with a hint to
      check ``--path``); any other non-MCP response is a ``warn`` (the host
      answered but does not look like an MCP server).

    Returns ``True`` when nothing failed (warnings do not fail the check), so a
    caller can use the boolean as an exit-code gate.
    """
    url = upstream.rstrip("/") + mcp_path
    print(f"mcp-persist-proxy check: {url}\n")

    owns_client = client is None
    client = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
    try:
        try:
            request = client.build_request(
                "POST",
                url,
                json=_CHECK_INIT_REQUEST,
                headers={"accept": "application/json, text/event-stream"},
            )
            response = await client.send(request, stream=True)
        except httpx.HTTPError as exc:
            _print_check("fail", "reachable", f"could not connect to {url}: {exc}")
            print("\nUpstream is not reachable.")
            return False

        try:
            _print_check("ok", "reachable", f"upstream responded (HTTP {response.status_code})")
            ok = _classify_handshake(response, mcp_path, await _maybe_read_json(response))
        finally:
            await response.aclose()
    finally:
        if owns_client:
            await client.aclose()

    print("\nUpstream looks ready to proxy." if ok else "\nUpstream check failed.")
    return ok


async def _maybe_read_json(response: httpx.Response) -> bytes:
    """Read the body only for a JSON response; never drain an open SSE stream."""
    if "application/json" in response.headers.get("content-type", "").lower():
        return await response.aread()
    return b""


def _classify_handshake(response: httpx.Response, mcp_path: str, json_body: bytes) -> bool:
    """Classify the initialize response into the streamable-http check. Returns ok."""
    ctype = response.headers.get("content-type", "").lower()
    if response.status_code in (404, 405):
        _print_check(
            "fail",
            "streamable-http",
            f"no MCP endpoint at {mcp_path} (HTTP {response.status_code}); check --path",
        )
        return False
    if "text/event-stream" in ctype:
        _print_check("ok", "streamable-http", "upstream speaks MCP Streamable HTTP (text/event-stream)")
        return True
    if b'"jsonrpc"' in json_body:
        _print_check("ok", "streamable-http", "upstream speaks MCP Streamable HTTP (JSON-RPC response)")
        return True
    _print_check(
        "warn",
        "streamable-http",
        f"reachable but response did not look like MCP (HTTP {response.status_code}, content-type {ctype or 'none'})",
    )
    return True


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
