"""Package-level tests: public API surface and version metadata."""

from __future__ import annotations

import mcp_persist


def test_version_is_exposed():
    # Resolved from installed package metadata; assert it's present and sane
    # rather than hard-coding the number (which would need updating every bump).
    assert isinstance(mcp_persist.__version__, str)
    assert mcp_persist.__version__


def test_public_api_is_exported():
    assert set(mcp_persist.__all__) >= {
        "RedisEventStore",
        "SQLiteEventStore",
        "PostgresEventStore",
        "__version__",
    }
