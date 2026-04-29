"""Test that bulk vendor import writes the CSV's vendor email and the
client's portal email to separate fields on each vendor record."""

import csv
import tempfile
from unittest.mock import MagicMock, patch, call


def test_imported_vendor_has_vendor_and_client_emails():
    """Vendor record created during CSV import should populate two distinct
    email fields: Vendor Email (from CSV) and Client Email
    (fldyPBBSXqYnvjhL0, copy of client portal email for Softr filtering)."""

    # Prepare a tiny CSV
    csv_content = "vendor_name,email\nTest Plumbing LLC,plumber@example.com\n"
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="")
    tmp.write(csv_content)
    tmp.flush()
    tmp_path = tmp.name
    tmp.close()

    # Mock Airtable tables
    mock_api = MagicMock()

    mock_clients_table = MagicMock()
    mock_clients_table.all.return_value = [{
        "id": "recCLIENT1",
        "fields": {
            "Client Name": "Dalton's Trucking",
            "fldmh1sYahgN5x6KQ": "dalton@example.com",
        },
    }]

    mock_vendors_table = MagicMock()
    mock_vendors_table.create.return_value = {"id": "recVENDOR1", "fields": {}}

    mock_junction_table = MagicMock()
    mock_junction_table.all.return_value = []

    mock_assignments_table = MagicMock()

    def table_router(base_id, table_name):
        if table_name == "Clients":
            return mock_clients_table
        elif table_name == "Vendors":
            return mock_vendors_table
        elif table_name == "tblYPs2h9jxT3OL9H":
            return mock_junction_table
        elif table_name == "tblpYKywfs0YHiQ98":
            return mock_assignments_table
        return MagicMock()

    mock_api.table.side_effect = table_router

    with patch("bulk_vendor_import.Api", return_value=mock_api):
        import bulk_vendor_import
        bulk_vendor_import.import_vendors("Dalton's Trucking", tmp_path)

    # Verify vendors_table.create was called with the portal email field
    assert mock_vendors_table.create.call_count == 1
    created_fields = mock_vendors_table.create.call_args[0][0]

    assert created_fields["Vendor Name"] == "Test Plumbing LLC"
    assert created_fields["Vendor Email"] == "plumber@example.com", (
        f"Expected vendor email 'plumber@example.com', got '{created_fields.get('Vendor Email')}'"
    )
    assert created_fields["fldyPBBSXqYnvjhL0"] == "dalton@example.com", (
        f"Expected client portal email 'dalton@example.com' in Client Email field, "
        f"got '{created_fields.get('fldyPBBSXqYnvjhL0')}'"
    )
    print("Test passed: imported vendor has CSV email in Vendor Email and "
          "client portal email in Client Email (fldyPBBSXqYnvjhL0).")


if __name__ == "__main__":
    test_imported_vendor_has_vendor_and_client_emails()
