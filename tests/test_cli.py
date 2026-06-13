"""Tests for the mcp-persist-proxy CLI argument handling and process helpers.

The server-running paths (uvicorn, real subprocesses) are not exercised here —
they bind ports and are covered by manual/integration use. These tests pin down
the pure logic: argument splitting on ``--``, mode detection, the readiness
poll, and child termination.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from mcp_persist import _cli


def test_parse_args_mode1_running_upstream():
    args, command = _cli._parse_args(
        ["--upstream", "http://host:1", "--backend", "sqlite", "--url", "e.db", "--port", "9000"]
    )
    assert args.upstream == "http://host:1"
    assert (args.backend, args.url, args.port) == ("sqlite", "e.db", 9000)
    assert command == []


def test_parse_args_mode2_splits_on_double_dash():
    args, command = _cli._parse_args(
        [
            "--backend",
            "redis",
            "--url",
            "redis://h",
            "--upstream-port",
            "8123",
            "--",
            "uvicorn",
            "app:x",
            "--port",
            "8123",
        ]
    )
    assert args.upstream is None
    assert args.upstream_port == 8123
    # Everything after "--" is the child command, including its own --port flag.
    assert command == ["uvicorn", "app:x", "--port", "8123"]


def test_parse_args_defaults():
    args, command = _cli._parse_args([])
    assert (args.host, args.port, args.path, args.upstream_port) == ("0.0.0.0", 8000, "/mcp", 8001)
    assert args.upstream is None and command == []


def test_main_errors_without_a_mode(monkeypatch, capsys):
    monkeypatch.setattr(_cli.sys, "argv", ["mcp-persist-proxy"])
    with pytest.raises(SystemExit) as excinfo:
        _cli.main()
    assert excinfo.value.code == 2
    assert "provide --upstream URL or a command after --" in capsys.readouterr().err


class _FakeClient:
    def __init__(self, fail_times: int) -> None:
        self._fail_times = fail_times
        self.calls = 0

    async def __aenter__(self) -> _FakeClient:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def get(self, url: str, timeout: float | None = None) -> None:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise httpx.ConnectError("not up yet")


@pytest.mark.anyio
async def test_wait_until_ready_returns_after_upstream_answers(monkeypatch):
    client = _FakeClient(fail_times=2)  # fails twice, then succeeds
    monkeypatch.setattr(_cli.httpx, "AsyncClient", lambda *a, **k: client)
    await asyncio.wait_for(_cli._wait_until_ready("http://up", timeout=5.0), 5)
    assert client.calls == 3


@pytest.mark.anyio
async def test_wait_until_ready_times_out(monkeypatch):
    monkeypatch.setattr(_cli.httpx, "AsyncClient", lambda *a, **k: _FakeClient(fail_times=10_000))
    with pytest.raises(RuntimeError, match="did not become ready"):
        await _cli._wait_until_ready("http://up", timeout=0.3)


def test_parse_args_check_flag():
    args, _ = _cli._parse_args(["--upstream", "http://host:1", "--check"])
    assert args.check is True
    args, _ = _cli._parse_args(["--upstream", "http://host:1"])
    assert args.check is False


def test_main_check_requires_upstream(monkeypatch, capsys):
    monkeypatch.setattr(_cli.sys, "argv", ["mcp-persist-proxy", "--check"])
    with pytest.raises(SystemExit) as excinfo:
        _cli.main()
    assert excinfo.value.code == 2
    assert "--check requires --upstream" in capsys.readouterr().err


def _check_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.anyio
async def test_check_upstream_ok_sse(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/mcp"
        return httpx.Response(200, headers={"content-type": "text/event-stream"})

    async with _check_client(handler) as client:
        ok = await _cli._check_upstream("http://up:8001", "/mcp", client=client)
    out = capsys.readouterr().out
    assert ok is True
    assert "[ ok ] reachable" in out
    assert "Streamable HTTP (text/event-stream)" in out
    assert "ready to proxy" in out


@pytest.mark.anyio
async def test_check_upstream_ok_jsonrpc(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={"jsonrpc": "2.0", "id": "x", "result": {}},
        )

    async with _check_client(handler) as client:
        ok = await _cli._check_upstream("http://up:8001", "/mcp", client=client)
    out = capsys.readouterr().out
    assert ok is True
    assert "Streamable HTTP (JSON-RPC response)" in out


@pytest.mark.anyio
async def test_check_upstream_wrong_path_fails(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    async with _check_client(handler) as client:
        ok = await _cli._check_upstream("http://up:8001", "/wrong", client=client)
    out = capsys.readouterr().out
    assert ok is False
    assert "[fail] streamable-http" in out
    assert "check --path" in out
    assert "check failed" in out


@pytest.mark.anyio
async def test_check_upstream_reachable_but_not_mcp_warns(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-type": "text/html"}, text="<html>hi</html>")

    async with _check_client(handler) as client:
        ok = await _cli._check_upstream("http://up:8001", "/mcp", client=client)
    out = capsys.readouterr().out
    # A warning does not fail the check.
    assert ok is True
    assert "[warn] streamable-http" in out
    assert "did not look like MCP" in out


@pytest.mark.anyio
async def test_check_upstream_unreachable_fails(capsys):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with _check_client(handler) as client:
        ok = await _cli._check_upstream("http://down:9999", "/mcp", client=client)
    out = capsys.readouterr().out
    assert ok is False
    assert "[fail] reachable" in out
    assert "could not connect" in out
    assert "not reachable" in out


class _FakeProc:
    def __init__(self, *, returncode: int | None = None, hang: bool = False) -> None:
        self.returncode = returncode
        self._hang = hang
        self.terminated = False
        self.killed = False

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        if self._hang and not self.killed:
            await asyncio.sleep(3600)
        self.returncode = 0
        return 0


@pytest.mark.anyio
async def test_terminate_noop_when_already_exited():
    proc = _FakeProc(returncode=0)
    await _cli._terminate(proc)
    assert not proc.terminated and not proc.killed


@pytest.mark.anyio
async def test_terminate_graceful():
    proc = _FakeProc()
    await _cli._terminate(proc)
    assert proc.terminated and not proc.killed


@pytest.mark.anyio
async def test_terminate_kills_on_timeout():
    proc = _FakeProc(hang=True)
    await _cli._terminate(proc, timeout=0.05)
    assert proc.terminated and proc.killed
