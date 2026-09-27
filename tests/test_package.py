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


def test_every_exported_name_resolves():
    missing = [name for name in mcp_persist.__all__ if not hasattr(mcp_persist, name)]
    assert missing == []


def test_records_api_is_exported():
    # 2.1.0 announced these as public API; RecordFlusher shipped without the
    # top-level export, so `from mcp_persist import RecordFlusher` failed.
    assert set(mcp_persist.__all__) >= {
        "Record",
        "RecordStore",
        "record_store_for",
        "SQLiteRecordStore",
        "RedisRecordStore",
        "PostgresRecordStore",
        "PayloadPolicy",
        "require_payload_encryption",
        "RecordFlusher",
    }
