# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
"""Integration test running the example SQLite server and smoke test."""

import asyncio
import http.client
import subprocess
import sys
import time
from pathlib import Path

import pytest


@pytest.mark.anyio
async def test_sqlite_server_smoke():
    root = Path(__file__).parent.parent
    server_path = root / "examples" / "sqlite_server.py"
    smoke_test_path = root / "examples" / "_smoke_test.py"

    # Remove any existing notes.db
    db_path = root / "notes.db"
    if db_path.exists():
        try:
            db_path.unlink()
        except Exception:
            pass

    # Start the server in a subprocess
    proc = subprocess.Popen(
        [sys.executable, str(server_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(root),
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
                # FastMCP SSE handler accepts requests on this path, returning HTTP statuses
                if res.status:
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
                "SQLite example server failed to start within timeout.\n"
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

        # Clean up database
        if db_path.exists():
            try:
                db_path.unlink()
            except Exception:
                pass
