# test_module_15_email_queue_builder.py
"""
Unit tests for the idempotency check in module_15_email_queue_builder.

Verifies that when the same email parameters are passed twice,
only one Email Queue record is created.
"""

import sys
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

# Stub out external dependencies that may not be installed in test environments
for mod_name in [
    "pyairtable", "dotenv", "module_12_vendor_reminder_engine",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# Module under test
import module_15_email_queue_builder as m15


class TestCheckDuplicateEmail(unittest.TestCase):
    """Tests for the check_duplicate_email idempotency function."""

    @patch("module_15_email_queue_builder.connect_to_airtable")
    def test_no_duplicate_when_queue_is_empty(self, mock_connect):
        """If no matching records exist, check_duplicate_email returns False."""
        mock_table = MagicMock()
        mock_table.all.return_value = []
        mock_connect.return_value = mock_table

        result = m15.check_duplicate_email(
            recipient_email="sub@example.com",
            email_type="non-compliance notice",
            vendor_id="recABC123",
        )
        self.assertFalse(result)
        mock_table.all.assert_called_once()

    @patch("module_15_email_queue_builder.connect_to_airtable")
    def test_duplicate_detected_when_matching_record_exists(self, mock_connect):
        """If a matching record was created within 24h, returns True."""
        mock_table = MagicMock()
        mock_table.all.return_value = [
            {"id": "recEXISTING", "fields": {"Primary Email": "sub@example.com"}}
        ]
        mock_connect.return_value = mock_table

        result = m15.check_duplicate_email(
            recipient_email="sub@example.com",
            email_type="non-compliance notice",
            vendor_id="recABC123",
        )
        self.assertTrue(result)

    @patch("module_15_email_queue_builder.connect_to_airtable")
    def test_no_duplicate_for_different_email_type(self, mock_connect):
        """Different email_type should not be considered a duplicate."""
        mock_table = MagicMock()
        # Return empty — the Airtable formula filters by email_type
        mock_table.all.return_value = []
        mock_connect.return_value = mock_table

        result = m15.check_duplicate_email(
            recipient_email="sub@example.com",
            email_type="reminder",
            vendor_id="recABC123",
        )
        self.assertFalse(result)

    @patch("module_15_email_queue_builder.connect_to_airtable")
    def test_empty_email_returns_false(self, mock_connect):
        """If recipient_email is empty, skip the check and return False."""
        result = m15.check_duplicate_email(
            recipient_email="",
            email_type="non-compliance notice",
            vendor_id="recABC123",
        )
        self.assertFalse(result)
        mock_connect.assert_not_called()

    @patch("module_15_email_queue_builder.connect_to_airtable")
    def test_check_uses_cert_id_over_vendor_id(self, mock_connect):
        """When both cert_id and vendor_id are provided, cert_id takes precedence."""
        mock_table = MagicMock()
        mock_table.all.return_value = []
        mock_connect.return_value = mock_table

        m15.check_duplicate_email(
            recipient_email="sub@example.com",
            email_type="non-compliance notice",
            vendor_id="recVENDOR1",
            cert_id="recCERT99",
        )

        call_args = mock_table.all.call_args
        formula_used = call_args[1].get("formula") or call_args[0][0] if call_args[0] else call_args[1]["formula"]
        self.assertIn("recCERT99", formula_used)
        self.assertNotIn("recVENDOR1", formula_used)

    @patch("module_15_email_queue_builder.connect_to_airtable")
    def test_airtable_error_fails_open(self, mock_connect):
        """If the Airtable query throws, fail open (return False) so pipeline continues."""
        mock_table = MagicMock()
        mock_table.all.side_effect = Exception("Airtable rate limit")
        mock_connect.return_value = mock_table

        result = m15.check_duplicate_email(
            recipient_email="sub@example.com",
            email_type="non-compliance notice",
            vendor_id="recABC123",
        )
        self.assertFalse(result)


class TestCreateEmailQueueRecordIdempotency(unittest.TestCase):
    """End-to-end test: calling create_email_queue_record twice with the same
    parameters should only create ONE Airtable record."""

    @patch("module_15_email_queue_builder.connect_to_airtable")
    def test_second_identical_call_is_suppressed(self, mock_connect):
        """Simulate two calls: first creates, second is suppressed."""
        mock_table = MagicMock()
        mock_connect.return_value = mock_table

        vendor = {
            "id": "recVENDOR1",
            "fields": {
                "Vendor Name": "Acme Roofing",
                "Email": "acme@example.com",
            },
            "send_after": datetime.now(),
        }

        # First call: no existing records → should create
        mock_table.all.return_value = []
        m15.create_email_queue_record(
            vendor, "Insurance Update Required", "Please update your COI.",
            email_type="non-compliance notice",
        )
        mock_table.create.assert_called_once()

        # Reset mock
        mock_table.reset_mock()

        # Second call: existing record found → should NOT create
        mock_table.all.return_value = [
            {"id": "recEXISTING", "fields": {"Primary Email": "acme@example.com"}}
        ]
        m15.create_email_queue_record(
            vendor, "Insurance Update Required", "Please update your COI.",
            email_type="non-compliance notice",
        )
        mock_table.create.assert_not_called()


if __name__ == "__main__":
    unittest.main()
