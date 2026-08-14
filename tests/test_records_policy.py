"""The payload allowlist captures only what an operator explicitly named.

These are the security-critical tests of the record surface. Tool arguments are
arbitrary third-party input, so the interesting cases are all about what must
*not* end up in the store.
"""

from __future__ import annotations

import pytest

from mcp_persist.records import PayloadPolicy, require_payload_encryption


def test_default_policy_captures_nothing() -> None:
    policy = PayloadPolicy.off()
    assert policy.is_off is True

    payload, truncated = policy.select("tools/call", {"arguments": {"query": "hello"}}, tool_name="search")
    assert payload is None
    assert truncated is False


def test_allowed_tool_argument_is_captured() -> None:
    policy = PayloadPolicy(tool_arguments={"search": ["query"]})

    payload, truncated = policy.select(
        "tools/call", {"arguments": {"query": "kittens", "api_key": "sk-secret"}}, tool_name="search"
    )

    assert payload == {"query": "kittens"}
    assert truncated is False


def test_unallowed_field_is_omitted_not_masked() -> None:
    """A masked key still leaks its name, so the key must be absent entirely."""
    policy = PayloadPolicy(tool_arguments={"search": ["query"]})

    payload, _ = policy.select(
        "tools/call", {"arguments": {"query": "q", "patient_ssn": "123-45-6789"}}, tool_name="search"
    )

    assert payload is not None
    assert "patient_ssn" not in payload
    assert "***" not in str(payload)


def test_another_tools_allowlist_does_not_apply() -> None:
    policy = PayloadPolicy(tool_arguments={"search": ["query"]})

    payload, _ = policy.select("tools/call", {"arguments": {"query": "q"}}, tool_name="delete_account")

    assert payload is None


def test_top_level_key_does_not_capture_a_nested_object() -> None:
    """Allowing `config` must not sweep up whatever a tool author nested in it."""
    policy = PayloadPolicy(tool_arguments={"deploy": ["config"]})

    payload, truncated = policy.select(
        "tools/call",
        {"arguments": {"config": {"region": "eu", "secret_key": "sk-live-xxx"}}},
        tool_name="deploy",
    )

    assert payload is None
    assert truncated is True


def test_dot_path_reaches_one_named_nested_scalar() -> None:
    policy = PayloadPolicy(tool_arguments={"deploy": ["config.region"]})

    payload, _ = policy.select(
        "tools/call",
        {"arguments": {"config": {"region": "eu", "secret_key": "sk-live-xxx"}}},
        tool_name="deploy",
    )

    assert payload == {"config.region": "eu"}


def test_list_values_are_dropped() -> None:
    policy = PayloadPolicy(tool_arguments={"batch": ["items"]})

    payload, truncated = policy.select("tools/call", {"arguments": {"items": [1, 2, 3]}}, tool_name="batch")

    assert payload is None
    assert truncated is True


def test_value_is_truncated_and_flagged() -> None:
    policy = PayloadPolicy(tool_arguments={"echo": ["text"]}, max_value_bytes=10)

    payload, truncated = policy.select("tools/call", {"arguments": {"text": "x" * 500}}, tool_name="echo")

    assert payload == {"text": "x" * 10}
    assert truncated is True


def test_record_cap_drops_fields() -> None:
    policy = PayloadPolicy(tool_arguments={"wide": ["a", "b", "c"]}, max_value_bytes=1000, max_record_bytes=40)

    payload, truncated = policy.select(
        "tools/call", {"arguments": {"a": "x" * 30, "b": "y" * 30, "c": "z" * 30}}, tool_name="wide"
    )

    assert truncated is True
    assert payload is None or len(payload) < 3


def test_non_tool_method_uses_method_params() -> None:
    policy = PayloadPolicy(method_params={"resources/read": ["uri"]})

    payload, _ = policy.select("resources/read", {"uri": "file:///etc/hosts", "token": "t"})

    assert payload == {"uri": "file:///etc/hosts"}


def test_method_allowlist_does_not_leak_into_other_methods() -> None:
    policy = PayloadPolicy(method_params={"resources/read": ["uri"]})

    payload, _ = policy.select("prompts/get", {"uri": "file:///secret"})

    assert payload is None


def test_missing_key_is_not_invented() -> None:
    policy = PayloadPolicy(tool_arguments={"search": ["query", "absent"]})

    payload, _ = policy.select("tools/call", {"arguments": {"query": "q"}}, tool_name="search")

    assert payload == {"query": "q"}


def test_explicit_null_is_preserved_but_missing_is_not() -> None:
    policy = PayloadPolicy(tool_arguments={"search": ["query"]})

    payload, _ = policy.select("tools/call", {"arguments": {"query": None}}, tool_name="search")

    assert payload == {"query": None}


def test_empty_params_capture_nothing() -> None:
    policy = PayloadPolicy(tool_arguments={"search": ["query"]})

    assert policy.select("tools/call", None, tool_name="search") == (None, False)
    assert policy.select("tools/call", {}, tool_name="search") == (None, False)


def test_invalid_caps_are_rejected() -> None:
    with pytest.raises(ValueError):
        PayloadPolicy(max_value_bytes=0)
    with pytest.raises(ValueError):
        PayloadPolicy(max_record_bytes=-1)


class _StoreWithoutKeyring:
    _keyring = None


class _StoreWithKeyring:
    _keyring = object()


def test_payload_capture_without_a_keyring_is_refused() -> None:
    policy = PayloadPolicy(tool_arguments={"search": ["query"]})

    with pytest.raises(ValueError, match="plaintext"):
        require_payload_encryption(_StoreWithoutKeyring(), policy)


def test_payload_capture_without_a_keyring_can_be_accepted_explicitly() -> None:
    policy = PayloadPolicy(tool_arguments={"search": ["query"]})

    require_payload_encryption(_StoreWithoutKeyring(), policy, allow_plaintext=True)


def test_policy_off_needs_no_keyring() -> None:
    require_payload_encryption(_StoreWithoutKeyring(), PayloadPolicy.off())


def test_keyring_present_is_allowed() -> None:
    policy = PayloadPolicy(tool_arguments={"search": ["query"]})

    require_payload_encryption(_StoreWithKeyring(), policy)
