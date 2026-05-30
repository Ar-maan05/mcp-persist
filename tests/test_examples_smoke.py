# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Integration test running the example servers and smoke tests."""

import asyncio
import http.client
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest


def is_port_open(host: str, port: int) -> bool:
    import socket

    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


@pytest.mark.parametrize("backend", ["sqlite", "redis", "postgres"])
@pytest.mark.anyio
async def test_server_smoke(backend: str):
    root = Path(__file__).parent.parent
    server_path = root / "examples" / f"{backend}_server.py"
    smoke_test_path = root / "examples" / "_smoke_test.py"

    # Pre-checks and skips for external databases
    if backend == "redis":
        if not is_port_open("127.0.0.1", 6379):
            pytest.skip("Local Redis server not running on port 6379")
    elif backend == "postgres":
        if not is_port_open("127.0.0.1", 5432):
            pytest.skip("Local Postgres server not running on port 5432")

    # Remove any existing notes.db for SQLite
    db_path = root / "notes.db"
    if backend == "sqlite" and db_path.exists():
        try:
            db_path.unlink()
        except Exception:
            pass

    # Build environment dict with DATABASE_URL for Postgres
    env = os.environ.copy()
    if backend == "postgres":
        env["DATABASE_URL"] = os.environ.get(
            "MCP_TEST_POSTGRES_URL", "postgresql://postgres@localhost:5432/postgres"
        )

    # Start the server in a subprocess
    proc = subprocess.Popen(
        [sys.executable, str(server_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(root),
        env=env,
    )

    try:
        # Wait for the server to start (polling http://127.0.0.1:8000/mcp)
        start_time = time.time()
        connected = False
        while time.time() - start_time < 10.0:
            try:
                conn = http.client.HTTPConnection("127.0.0.1", 8000, timeout=1.0)
                conn.request("GET", "/mcp")
                res = conn.getresponse()
                # FastMCP SSE handler redirects or responds on this path
                if res.status in (200, 307, 400, 404, 405):
                    connected = True
                    conn.close()
                    break
            except Exception:
                pass
            await asyncio.sleep(0.1)

        if not connected:
            proc.terminate()
            stdout, stderr = proc.communicate(timeout=2.0)
            assert False, (
                f"{backend.capitalize()} example server failed to start within timeout.\n"
                f"STDOUT:\n{stdout.decode('utf-8', errors='replace')}\n"
                f"STDERR:\n{stderr.decode('utf-8', errors='replace')}"
            )

        # Now run the smoke test script in a subprocess
        smoke_proc = subprocess.run(
            [sys.executable, str(smoke_test_path)],
            capture_output=True,
            text=True,
            cwd=str(root),
        )

        # Assert exit code 0
        assert smoke_proc.returncode == 0, (
            f"Smoke test failed with exit code {smoke_proc.returncode}\n"
            f"STDOUT:\n{smoke_proc.stdout}\n"
            f"STDERR:\n{smoke_proc.stderr}"
        )

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()

        # Clean up database file
        if backend == "sqlite" and db_path.exists():
            try:
                db_path.unlink()
            except Exception:
                pass
