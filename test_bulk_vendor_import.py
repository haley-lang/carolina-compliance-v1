"""Tests for bulk vendor import:

- Vendor records hold the CSV's vendor email; the Client Email field
  (fldyPBBSXqYnvjhL0) is computed by Airtable from the linked Client and
  must NOT be written by the import script.
- Re-importing the same CSV against the same client must dedup via
  Vendor Client Assignments — second pass creates zero new records.
"""

import re
import tempfile
from unittest.mock import MagicMock, patch


def _build_stateful_mocks(client_id, client_portal_email):
    """Return (mock_api, state_dict) — Airtable mocks with stateful create/all
    so a test can run import_vendors twice and observe dedup behavior.

    state_dict exposes:
      - "vendors": list of created vendor records
      - "assignments": list of created assignment records
      - "junctions": list of created junction records
      - "vendor_create_calls" / "assignment_create_calls": counters
    """
    state = {
        "vendors": [],
        "assignments": [],
        "junctions": [],
        "vendor_create_calls": 0,
        "assignment_create_calls": 0,
    }

    mock_clients_table = MagicMock()
    mock_clients_table.all.return_value = [{
        "id": client_id,
        "fields": {
            "Client Name": "Dalton's Trucking",
            "fldmh1sYahgN5x6KQ": client_portal_email,
        },
    }]

    # vendors_table.create — auto-id, append to state
    def vendors_create(fields, **_):
        state["vendor_create_calls"] += 1
        rec = {"id": f"recVENDOR{state['vendor_create_calls']}", "fields": dict(fields)}
        state["vendors"].append(rec)
        return rec

    # vendors_table.all(formula=...) — parse the LOWER() = '...' formula and
    # return matches against the stateful vendor list.
    def vendors_all(formula=None, **_):
        if not formula:
            return list(state["vendors"])
        m = re.search(r"=\s*'([^']*)'", formula)
        if not m:
            return []
        target = m.group(1)
        return [
            v for v in state["vendors"]
            if (v["fields"].get("Vendor Name") or "").strip().lower() == target
        ]

    mock_vendors_table = MagicMock()
    mock_vendors_table.create.side_effect = vendors_create
    mock_vendors_table.all.side_effect = vendors_all

    # assignments_table.create — append to state
    def assignments_create(fields, **_):
        state["assignment_create_calls"] += 1
        rec = {"id": f"recASSIGN{state['assignment_create_calls']}", "fields": dict(fields)}
        state["assignments"].append(rec)
        return rec

    mock_assignments_table = MagicMock()
    mock_assignments_table.create.side_effect = assignments_create
    mock_assignments_table.all.side_effect = lambda **_: list(state["assignments"])

    # Orphan junction table — accept writes, never read
    mock_junction_table = MagicMock()
    mock_junction_table.create.side_effect = lambda fields, **_: state["junctions"].append(fields) or {"id": "recJUNC"}
    mock_junction_table.all.return_value = []

    def table_router(_base_id, table_name):
        if table_name == "Clients":
            return mock_clients_table
        if table_name == "Vendors":
            return mock_vendors_table
        if table_name == "tblYPs2h9jxT3OL9H":
            return mock_junction_table
        if table_name == "tblpYKywfs0YHiQ98":
            return mock_assignments_table
        return MagicMock()

    mock_api = MagicMock()
    mock_api.table.side_effect = table_router
    return mock_api, state


def _write_csv(content):
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="")
    tmp.write(content)
    tmp.flush()
    tmp.close()
    return tmp.name


def test_imported_vendor_writes_vendor_email_only():
    """Vendor record should hold the CSV email in Vendor Email and must NOT
    write to Client Email (fldyPBBSXqYnvjhL0) — that field is a computed
    lookup that auto-populates from the linked Client via the assignment."""

    csv_path = _write_csv("vendor_name,email\nTest Plumbing LLC,plumber@example.com\n")
    mock_api, state = _build_stateful_mocks("recCLIENT1", "dalton@example.com")

    with patch("bulk_vendor_import.Api", return_value=mock_api):
        import bulk_vendor_import
        bulk_vendor_import.import_vendors("Dalton's Trucking", csv_path)

    assert state["vendor_create_calls"] == 1
    created_fields = state["vendors"][0]["fields"]

    assert created_fields["Vendor Name"] == "Test Plumbing LLC"
    assert created_fields["Vendor Email"] == "plumber@example.com", (
        f"Expected vendor email 'plumber@example.com', got '{created_fields.get('Vendor Email')}'"
    )
    assert "fldyPBBSXqYnvjhL0" not in created_fields, (
        "Client Email (fldyPBBSXqYnvjhL0) is a computed lookup field — "
        "the import script must not write to it. Found value: "
        f"{created_fields.get('fldyPBBSXqYnvjhL0')!r}"
    )
    print("Test passed: vendor email correct, Client Email field not written.")


def test_dedup_skips_already_assigned_vendors_on_reimport():
    """Re-importing the same CSV against the same client must skip every row
    via the Vendor Client Assignments dedup. Second pass creates zero new
    Vendor or Assignment records."""

    csv_content = (
        "vendor_name,email\n"
        "Apex Roofing Services LLC,apex@example.fake\n"
        "Carolina Concrete Pros LLC,concrete@example.fake\n"
        "Piedmont Plumbing & Mechanical Inc,piedmont@example.fake\n"
    )
    csv_path = _write_csv(csv_content)
    mock_api, state = _build_stateful_mocks("recCLIENT1", "dalton@example.com")

    with patch("bulk_vendor_import.Api", return_value=mock_api):
        import bulk_vendor_import

        # First import — should create 3 vendors + 3 assignments
        bulk_vendor_import.import_vendors("Dalton's Trucking", csv_path)
        first_vendor_creates = state["vendor_create_calls"]
        first_assignment_creates = state["assignment_create_calls"]

        assert first_vendor_creates == 3, f"Expected 3 vendor creates on first import, got {first_vendor_creates}"
        assert first_assignment_creates == 3, f"Expected 3 assignment creates on first import, got {first_assignment_creates}"

        # Second import with the same CSV — dedup should skip all 3
        bulk_vendor_import.import_vendors("Dalton's Trucking", csv_path)

    assert state["vendor_create_calls"] == first_vendor_creates, (
        f"Expected 0 new vendor creates on re-import (still {first_vendor_creates}); "
        f"got {state['vendor_create_calls']}. Dedup against Vendor Client Assignments failed."
    )
    assert state["assignment_create_calls"] == first_assignment_creates, (
        f"Expected 0 new assignment creates on re-import (still {first_assignment_creates}); "
        f"got {state['assignment_create_calls']}."
    )
    print("Test passed: re-import skipped all 3 already-assigned vendors.")


if __name__ == "__main__":
    test_imported_vendor_writes_vendor_email_only()
    test_dedup_skips_already_assigned_vendors_on_reimport()
