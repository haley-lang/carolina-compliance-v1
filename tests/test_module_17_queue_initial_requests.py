"""Tests for module_17 — locks in two invariants that previously broke silently:

1. Email Queue rows MUST be created with both Vendor link AND Client link
   populated. The Client link is what module_18 uses to look up the GC's
   Requirements Status before sending.

2. The field names used to write the link arrays MUST match the constants
   from airtable_constants (EQ_VENDOR_LINK / EQ_CLIENT_LINK), not legacy
   strings like "Vendor Link" / "Client Link".
"""

from unittest.mock import MagicMock

from airtable_constants import EQ_VENDOR_LINK, EQ_CLIENT_LINK


def test_create_queue_record_writes_vendor_and_client_links():
    """_create_queue_record must populate both link fields with the
    correct field-name constants and as single-element record-ID lists."""
    from module_17_queue_initial_requests import _create_queue_record

    mock_eq_table = MagicMock()
    mock_eq_table.create.return_value = {"id": "recQUEUE1", "fields": {}}

    _create_queue_record(
        email_queue_table=mock_eq_table,
        vendor_id="recVENDOR1",
        vendor_name="Acme Roofing",
        vendor_email="vendor@example.com",
        client_name="Bridges Construction Group, LLC",
        client_id="recCLIENT1",
    )

    assert mock_eq_table.create.call_count == 1
    written_fields = mock_eq_table.create.call_args[0][0]

    assert EQ_VENDOR_LINK in written_fields, (
        f"Queue row missing {EQ_VENDOR_LINK!r} — module_18's duplicate-send guard "
        "won't be able to resolve vendor_record_id."
    )
    assert written_fields[EQ_VENDOR_LINK] == ["recVENDOR1"]

    assert EQ_CLIENT_LINK in written_fields, (
        f"Queue row missing {EQ_CLIENT_LINK!r} — module_18's "
        "_client_requirements_received guard will short-circuit to False, "
        "silently skipping every Initial Request with a misleading "
        "'client requirements not yet confirmed' log."
    )
    assert written_fields[EQ_CLIENT_LINK] == ["recCLIENT1"]


def test_create_queue_record_skips_client_link_when_id_unknown():
    """If the resolver couldn't find a client_id (no active assignment),
    the queue row is created without a Client link rather than with [None]
    or an empty list — which would error on Airtable's side."""
    from module_17_queue_initial_requests import _create_queue_record

    mock_eq_table = MagicMock()
    mock_eq_table.create.return_value = {"id": "recQUEUE1", "fields": {}}

    _create_queue_record(
        email_queue_table=mock_eq_table,
        vendor_id="recVENDOR1",
        vendor_name="Acme Roofing",
        vendor_email="vendor@example.com",
        client_name="[Client Name]",
        client_id=None,
    )

    written_fields = mock_eq_table.create.call_args[0][0]
    assert EQ_VENDOR_LINK in written_fields
    assert EQ_CLIENT_LINK not in written_fields, (
        "When client_id is None the queue row must not write the Client link "
        "field at all — empty or [None] arrays cause Airtable 422."
    )


def test_resolve_client_returns_name_and_id_tuple():
    """_resolve_client must return BOTH name and id so the queue row can
    populate its Client link. Returning only the name (the prior bug)
    leaves the link unpopulated."""
    from module_17_queue_initial_requests import _resolve_client

    mock_assignments_table = MagicMock()
    mock_assignments_table.all.return_value = [{
        "id": "recASSIGN1",
        "fields": {
            "Vendor Link": ["recVENDOR1"],
            "Client Link": ["recCLIENT1"],
            "Active": True,
        },
    }]

    mock_clients_table = MagicMock()
    mock_clients_table.get.return_value = {
        "id": "recCLIENT1",
        "fields": {"Client Name": "Bridges Construction Group, LLC"},
    }

    name, client_id = _resolve_client(
        vendor_id="recVENDOR1",
        vendor_name="Acme Roofing",
        vendor_fields={},
        clients_table=mock_clients_table,
        assignments_table=mock_assignments_table,
    )

    assert name == "Bridges Construction Group, LLC"
    assert client_id == "recCLIENT1"


def test_resolve_client_returns_placeholder_and_none_when_unresolvable():
    """When no assignment and no legacy link, both halves of the tuple
    fall back cleanly: placeholder name + None id (so the caller can
    branch on id is None)."""
    from module_17_queue_initial_requests import _resolve_client, CLIENT_PLACEHOLDER

    mock_assignments_table = MagicMock()
    mock_assignments_table.all.return_value = []

    mock_clients_table = MagicMock()

    name, client_id = _resolve_client(
        vendor_id="recVENDOR1",
        vendor_name="Acme Roofing",
        vendor_fields={},
        clients_table=mock_clients_table,
        assignments_table=mock_assignments_table,
    )

    assert name == CLIENT_PLACEHOLDER
    assert client_id is None
