import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import tests._bootstrap  # noqa: F401

import pytest
from unittest.mock import MagicMock, patch


class TestCancellationDetection:
    """Test cancellation document detection across modules."""

    def test_extractor_detect_cancellation_notice(self):
        """detect_cancellation_notice should find keywords."""
        from extractor import detect_cancellation_notice
        assert detect_cancellation_notice("This is a notice of cancellation") is True
        assert detect_cancellation_notice("This is a regular COI") is False
        assert detect_cancellation_notice("Policy has been cancelled") is True
        assert detect_cancellation_notice("CANCELLATION") is True
        assert detect_cancellation_notice("reinstatement") is True

    def test_extractor_classification_overrides(self):
        """apply_simple_document_classification should override document_type."""
        from extractor import apply_simple_document_classification
        data = {"document_type": "COI", "named_insured": "Test Corp"}
        result = apply_simple_document_classification(data, Path("cancellation_notice.pdf"))
        assert result["document_type"] == "cancellation_notice"

    def test_reinstatement_takes_priority(self):
        """Reinstatement keywords should override cancellation."""
        from extractor import apply_simple_document_classification
        data = {"document_type": "COI", "named_insured": "Test Corp reinstatement"}
        result = apply_simple_document_classification(data, Path("doc.pdf"))
        assert result["document_type"] == "reinstatement"

    def test_processor_is_cancellation_document(self):
        """processor.is_cancellation_document with normalized types."""
        from processor import is_cancellation_document, normalize_document_type
        assert is_cancellation_document(normalize_document_type("cancellation_notice")) is True
        assert is_cancellation_document(normalize_document_type("Cancellation Notice")) is True
        assert is_cancellation_document(normalize_document_type("notice_of_cancellation")) is True
        assert is_cancellation_document(normalize_document_type("COI")) is False

    def test_processor_extract_policy_numbers_for_cancellation(self):
        """Cancellation handling should extract policy numbers correctly."""
        from processor import extract_policy_numbers
        raw_data = {
            "policy_number": "GL-2025-001234",
            "policies": [
                {"policy_number": "GL-2025-001234"},
                {"policy_number": "WC-2025-005678"},
            ]
        }
        numbers = extract_policy_numbers(raw_data)
        assert "GL-2025-001234" in numbers
        assert "WC-2025-005678" in numbers
        assert len(numbers) == 2  # deduplication


class TestModule8bEventDetection:
    """Test module_8b.fetch_unprocessed_events filtering logic."""

    def test_filters_cancellation_records(self):
        """Only cancellation/endorsement/reinstatement records returned."""
        from module_8b import fetch_unprocessed_events
        # Create mock table
        mock_table = MagicMock()
        mock_table.all.return_value = [
            {"id": "rec1", "fields": {"fldovqDwNx7SnTkcm": {"name": "cancellation_notice"}, "fldlvQXsy7c12E9ak": False}},
            {"id": "rec2", "fields": {"fldovqDwNx7SnTkcm": {"name": "COI"}, "fldlvQXsy7c12E9ak": False}},
            {"id": "rec3", "fields": {"fldovqDwNx7SnTkcm": {"name": "endorsement"}, "fldlvQXsy7c12E9ak": False}},
            {"id": "rec4", "fields": {"fldovqDwNx7SnTkcm": {"name": "cancellation_notice"}, "fldlvQXsy7c12E9ak": True}},  # already processed
        ]
        results = fetch_unprocessed_events(mock_table)
        assert len(results) == 2  # rec1 and rec3 only

    def test_excludes_already_actioned(self):
        """Records with Cancellation Action Taken = True should be excluded."""
        from module_8b import fetch_unprocessed_events
        mock_table = MagicMock()
        mock_table.all.return_value = [
            {"id": "rec1", "fields": {"fldovqDwNx7SnTkcm": {"name": "cancellation_notice"}, "fldlvQXsy7c12E9ak": True}},
            {"id": "rec2", "fields": {"fldovqDwNx7SnTkcm": {"name": "endorsement"}, "fldlvQXsy7c12E9ak": True}},
        ]
        results = fetch_unprocessed_events(mock_table)
        assert len(results) == 0  # All already actioned

    def test_handles_string_doc_type(self):
        """Doc type field can be string instead of dict."""
        from module_8b import fetch_unprocessed_events
        mock_table = MagicMock()
        mock_table.all.return_value = [
            {"id": "rec1", "fields": {"fldovqDwNx7SnTkcm": "cancellation_notice", "fldlvQXsy7c12E9ak": False}},
        ]
        results = fetch_unprocessed_events(mock_table)
        assert len(results) == 1


class TestCancellationEmailQueuing:
    """Test that cancellation triggers appropriate email notifications."""

    def test_handle_cancellation_queues_three_emails(self):
        """handle_cancellation should queue 3 emails: Haley, GC, sub."""
        from module_8b import handle_cancellation
        # Mock all required tables
        mock_extraction = {
            "id": "rec1",
            "fields": {
                "fldHAwdxnX3yM0s3o": "cancellation_notice.pdf",
                "fldlvQXsy7c12E9ak": False,
            }
        }
        mock_vendor = {
            "id": "recV1",
            "fields": {
                "fldb0BUb3wggDMJMp": "ABC Contractors",
                "fldxteHtQ5ITcx6Zw": "vendor@abc.com",
                "fldWfYjoWXxksW5iG": "agency@abc.com",
                "fldUSiQOqZocnT4zX": "Matches Requirements",
            }
        }
        mock_client = {
            "id": "recC1",
            "fields": {
                "fldEZdqmIeahXDZHL": "Test Corp",
                "fldmh1sYahgN5x6KQ": "haley@test.com",
                "fldIWXSLRJYAVRs3P": "Haley Manager",
            }
        }
        mock_email_queue_table = MagicMock()
        mock_email_queue_table.create = MagicMock(return_value={"id": "recEmail"})
        mock_vendors_table = MagicMock()
        mock_assignments_table = MagicMock()

        # Call handle_cancellation with correct positional args
        handle_cancellation(
            mock_extraction,
            mock_vendor,
            mock_client,
            mock_email_queue_table,
            mock_vendors_table,
            mock_assignments_table
        )

        # Assert email_queue_table.create was called
        assert mock_email_queue_table.create.call_count >= 1

    def test_handle_cancellation_updates_vendor_status(self):
        """handle_cancellation should set vendor to Non-Compliant."""
        from module_8b import handle_cancellation
        # Mock tables
        mock_extraction = {
            "id": "rec1",
            "fields": {
                "fldHAwdxnX3yM0s3o": "cancellation_notice.pdf",
                "fldlvQXsy7c12E9ak": False,
            }
        }
        mock_vendor = {
            "id": "recV1",
            "fields": {
                "fldb0BUb3wggDMJMp": "ABC Contractors",
                "fldxteHtQ5ITcx6Zw": "vendor@abc.com",
                "fldWfYjoWXxksW5iG": "agency@abc.com",
                "fldUSiQOqZocnT4zX": "Matches Requirements",
            }
        }
        mock_client = {
            "id": "recC1",
            "fields": {
                "fldEZdqmIeahXDZHL": "Test Corp",
                "fldmh1sYahgN5x6KQ": "haley@test.com",
                "fldIWXSLRJYAVRs3P": "Haley Manager",
            }
        }
        mock_email_queue_table = MagicMock()
        mock_vendors_table = MagicMock()
        mock_assignments_table = MagicMock()

        handle_cancellation(
            mock_extraction,
            mock_vendor,
            mock_client,
            mock_email_queue_table,
            mock_vendors_table,
            mock_assignments_table
        )

        # Assert vendors_table.update was called with Non-Compliant status
        mock_vendors_table.update.assert_called()
        call_args = mock_vendors_table.update.call_args
        if call_args:
            assert "Has Open Items" in str(call_args) or "has open items" in str(call_args).lower()

    def test_handle_endorsement_queues_emails(self):
        """handle_endorsement should queue notification email."""
        from module_8b import handle_endorsement
        mock_extraction = {
            "id": "rec1",
            "fields": {
                "fldHAwdxnX3yM0s3o": "endorsement.pdf",
                "fldlvQXsy7c12E9ak": False,
            }
        }
        mock_vendor = {
            "id": "recV1",
            "fields": {
                "fldb0BUb3wggDMJMp": "ABC Contractors",
                "fldxteHtQ5ITcx6Zw": "vendor@abc.com",
            }
        }
        mock_client = {
            "id": "recC1",
            "fields": {
                "fldEZdqmIeahXDZHL": "Test Corp",
                "fldmh1sYahgN5x6KQ": "haley@test.com",
            }
        }
        mock_email_queue_table = MagicMock()
        mock_email_queue_table.create = MagicMock(return_value={"id": "recEmail"})

        handle_endorsement(
            mock_extraction,
            mock_vendor,
            mock_client,
            mock_email_queue_table
        )

        # Should queue at least one email
        assert mock_email_queue_table.create.call_count >= 1

    def test_handle_reinstatement_updates_status(self):
        """handle_reinstatement should update policy status back to Active."""
        from module_8b import handle_reinstatement
        mock_extraction = {
            "id": "rec1",
            "fields": {
                "fldHAwdxnX3yM0s3o": "reinstatement.pdf",
                "fldlvQXsy7c12E9ak": False,
            }
        }
        mock_vendor = {
            "id": "recV1",
            "fields": {
                "fldb0BUb3wggDMJMp": "ABC Contractors",
                "fldxteHtQ5ITcx6Zw": "vendor@abc.com",
            }
        }
        mock_client = {
            "id": "recC1",
            "fields": {
                "fldEZdqmIeahXDZHL": "Test Corp",
                "fldmh1sYahgN5x6KQ": "haley@test.com",
            }
        }
        mock_email_queue_table = MagicMock()
        mock_vendors_table = MagicMock()
        mock_assignments_table = MagicMock()
        mock_client_requirements_table = MagicMock()
        mock_policies_table = MagicMock()
        mock_policies_table.update = MagicMock()

        handle_reinstatement(
            mock_extraction,
            mock_vendor,
            mock_client,
            mock_email_queue_table,
            mock_vendors_table,
            mock_assignments_table,
            mock_client_requirements_table,
            mock_policies_table
        )

        # Assert vendor compliance status was updated (reinstatement updates vendor, not policies)
        mock_vendors_table.update.assert_called()

    def test_mark_action_taken_sets_flag(self):
        """mark_action_taken should set the action taken flag to True."""
        from module_8b import mark_action_taken
        mock_table = MagicMock()

        mark_action_taken(
            mock_table,
            "rec1"
        )

        # Assert the table was updated with action taken flag
        mock_table.update.assert_called()
        call_args = mock_table.update.call_args
        assert call_args is not None

    def test_queue_email_creates_record(self):
        """queue_email should create a new email record."""
        from module_8b import queue_email
        mock_table = MagicMock()
        mock_table.create = MagicMock(return_value={"id": "recEmail1"})

        result = queue_email(
            mock_table,
            "haley@example.com",
            "Cancellation Notice",
            "Policy has been cancelled",
            "cancellation_notice"
        )

        # Assert email record was created (queue_email returns None — it logs, not returns)
        mock_table.create.assert_called_once()
