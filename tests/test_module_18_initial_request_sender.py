"""Tests for module_18 — locks in the field-name invariants that previously
broke the queue-row reads.

The Email Queue's link fields are named "Vendor" and "Client" per
airtable_constants (EQ_VENDOR_LINK / EQ_CLIENT_LINK). Module_18 was
reading "Vendor Link" / "Client Link" — a silent mismatch that made
every Initial Request skip with a misleading log message.
"""

from unittest.mock import MagicMock, patch

from airtable_constants import EQ_VENDOR_LINK, EQ_CLIENT_LINK, C_REQUIREMENTS_STATUS


def test_client_requirements_received_uses_constant_field_name():
    """The function must read from the field name in airtable_constants
    (C_REQUIREMENTS_STATUS), not a hardcoded string that could drift."""
    from module_18_vendor_initial_request_sender import _client_requirements_received

    mock_clients_table = MagicMock()
    mock_clients_table.get.return_value = {
        "id": "recCLIENT1",
        "fields": {C_REQUIREMENTS_STATUS: "Received"},
    }
    mock_api = MagicMock()
    mock_api.table.return_value = mock_clients_table

    assert _client_requirements_received(mock_api, "recCLIENT1") is True


def test_client_requirements_received_returns_false_when_not_received():
    from module_18_vendor_initial_request_sender import _client_requirements_received

    mock_clients_table = MagicMock()
    mock_clients_table.get.return_value = {
        "id": "recCLIENT1",
        "fields": {C_REQUIREMENTS_STATUS: "Pending — Awaiting Reply"},
    }
    mock_api = MagicMock()
    mock_api.table.return_value = mock_clients_table

    assert _client_requirements_received(mock_api, "recCLIENT1") is False


def test_client_requirements_received_short_circuits_on_missing_id():
    """Empty/None client_record_id must return False without touching Airtable.
    This is the path that fires when the queue row's Client link is empty —
    we want fast-fail, not a misleading None lookup."""
    from module_18_vendor_initial_request_sender import _client_requirements_received

    mock_api = MagicMock()
    assert _client_requirements_received(mock_api, None) is False
    assert _client_requirements_received(mock_api, "") is False
    mock_api.table.assert_not_called()


def test_module_uses_correct_link_field_constants():
    """Regression guard: the source code must reference EQ_VENDOR_LINK and
    EQ_CLIENT_LINK constants (not the legacy 'Vendor Link' / 'Client Link'
    strings) in actual field-lookup calls. Catches the exact bug class
    that silently skipped every Initial Request."""
    import inspect
    import re
    import module_18_vendor_initial_request_sender as m18

    source = inspect.getsource(m18)
    # Strip comments and docstrings before scanning, so this test doesn't
    # catch explanatory text describing the bug it's guarding against.
    code_only_lines = []
    for line in source.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        # Drop inline comments
        line_no_inline = re.sub(r"\s+#.*$", "", line)
        code_only_lines.append(line_no_inline)
    code_only = "\n".join(code_only_lines)

    # The exact buggy usage pattern: a fields.get("Vendor Link"...) or
    # fields.get("Client Link"...) call. The fix replaced these with
    # fields.get(EQ_VENDOR_LINK, ...) / fields.get(EQ_CLIENT_LINK, ...).
    assert not re.search(r'\bget\(\s*"Vendor Link"', code_only), (
        "module_18 still calls fields.get(\"Vendor Link\", ...). The actual "
        "Email Queue field is named \"Vendor\" (EQ_VENDOR_LINK). This "
        "mismatch silently broke the duplicate-send guard."
    )
    assert not re.search(r'\bget\(\s*"Client Link"', code_only), (
        "module_18 still calls fields.get(\"Client Link\", ...). The actual "
        "Email Queue field is named \"Client\" (EQ_CLIENT_LINK). This "
        "mismatch silently broke the requirements-confirmed guard."
    )
    # And the imports must include the canonical constants.
    assert "EQ_VENDOR_LINK" in code_only
    assert "EQ_CLIENT_LINK" in code_only


def test_queue_row_field_lookups_resolve_link_arrays():
    """Simulate the exact queue-row read pattern from send_initial_vendor_requests:
    a queue row with link arrays under the EQ_VENDOR_LINK / EQ_CLIENT_LINK keys
    must resolve to the correct vendor_record_id and client_record_id."""
    queue_row_fields = {
        EQ_VENDOR_LINK: ["recVENDOR1"],
        EQ_CLIENT_LINK: ["recCLIENT1"],
    }

    vendor_link_raw = queue_row_fields.get(EQ_VENDOR_LINK, [])
    vendor_record_id = vendor_link_raw[0] if isinstance(vendor_link_raw, list) and vendor_link_raw else None
    client_link_raw = queue_row_fields.get(EQ_CLIENT_LINK, [])
    client_record_id = client_link_raw[0] if isinstance(client_link_raw, list) and client_link_raw else None

    assert vendor_record_id == "recVENDOR1"
    assert client_record_id == "recCLIENT1"


def test_queue_row_with_legacy_field_names_resolves_to_none():
    """If a queue row was written with the legacy 'Vendor Link' / 'Client Link'
    field names (which is what was happening pre-fix), the new constant-based
    lookup correctly returns None — making the guards skip rather than send
    blindly. Documents the failure mode of the previous buggy queue rows."""
    queue_row_fields_legacy = {
        "Vendor Link": ["recVENDOR1"],
        "Client Link": ["recCLIENT1"],
    }

    vendor_link_raw = queue_row_fields_legacy.get(EQ_VENDOR_LINK, [])
    client_link_raw = queue_row_fields_legacy.get(EQ_CLIENT_LINK, [])

    assert vendor_link_raw == [], "Legacy field name should not match new constant"
    assert client_link_raw == [], "Legacy field name should not match new constant"
