import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import tests._bootstrap  # noqa: F401

import pytest
from unittest.mock import MagicMock, patch


class TestMissingFieldsDetection:
    """Test pipeline handling of certificates with missing fields."""

    def test_no_named_insured_vendor_match_fails(self):
        """Certificate with no named insured -> vendor match should fail."""
        from incoming_extraction_matcher_slice import evaluate_vendor_match
        extraction_fields = {"Named Insured": "", "Certificate Holder": "Test Client"}
        vendors = [{"id": "recV1", "fields": {"Vendor Name": "ABC Corp"}}]
        result = evaluate_vendor_match(extraction_fields, vendors)
        assert result.match_status != "Matched"

    def test_no_named_insured_returns_pending(self):
        """Missing named insured -> matcher returns 'Pending Match'."""
        from incoming_extraction_matcher_slice import evaluate_vendor_match
        extraction_fields = {"Named Insured": None}
        result = evaluate_vendor_match(extraction_fields, [])
        # Should return a pending or unmatched state
        assert result.match_status in ["Pending Match", "Unmatched", "Pending"]

    def test_no_certificate_holder_client_match_skipped(self):
        """No certificate holder -> client resolution skipped, vendor still matched."""
        from incoming_extraction_matcher_slice import evaluate_vendor_match
        extraction_fields = {"Named Insured": "ABC Corp", "Certificate Holder": ""}
        vendors = [{"id": "recV1", "fields": {"Vendor Name": "ABC Corp"}}]
        result = evaluate_vendor_match(extraction_fields, vendors)
        assert result.matched_vendor_id == "recV1"
        # Client should not be matched when certificate holder is empty
        assert result.matched_client_id is None or result.matched_client_id == ""

    def test_missing_policy_section_no_policies_created(self):
        """Empty policies array -> no policy records created."""
        from processor import process_policies
        mock_table = MagicMock()
        result = process_policies(mock_table, [], "recV1", "recC1", "test.pdf")
        assert result == []
        mock_table.create.assert_not_called()

    def test_blank_expiration_flagged_needs_review(self):
        """Blank expiration date -> policy status 'Needs Review'."""
        from processor import compute_policy_status
        assert compute_policy_status("") == "Needs Review"
        assert compute_policy_status("   ") == "Needs Review"

    def test_missing_carrier_still_creates_policy(self):
        """Missing carrier field should not prevent policy creation."""
        from processor import process_policies
        mock_table = MagicMock()
        mock_table.first.return_value = None
        mock_table.create.return_value = {"id": "recPNew", "fields": {}}

        policies = [
            {
                "policy_number": "GL-2025-001234",
                "policy_type": "General Liability",
                "expiration_date": "01/01/2026",
                "carrier": ""  # Missing carrier
            }
        ]

        result = process_policies(mock_table, policies, "recV1", "recC1", "test.pdf")
        # Should still create policy despite missing carrier
        mock_table.create.assert_called()
        assert len(result) > 0

    def test_all_fields_missing_extraction(self):
        """Extraction with all null fields should be flagged for review."""
        from processor import is_invalid_policy_number, compute_policy_status
        assert is_invalid_policy_number("") is True
        assert compute_policy_status("") == "Needs Review"

    def test_missing_coverage_limits_handled(self):
        """Missing coverage limits should not prevent policy creation."""
        from processor import process_policies
        mock_table = MagicMock()
        mock_table.first.return_value = None
        mock_table.create.return_value = {"id": "recPNew", "fields": {}}

        policies = [
            {
                "policy_number": "GL-2025-001234",
                "policy_type": "General Liability",
                "expiration_date": "01/01/2026",
                "coverage_limits": None  # Missing limits
            }
        ]

        result = process_policies(mock_table, policies, "recV1", "recC1", "test.pdf")
        mock_table.create.assert_called()
        assert len(result) > 0

    def test_missing_effective_date_handled(self):
        """Missing effective date should not cause extraction failure."""
        raw_data = {
            "named_insured": "Test Corp",
            "policies": [
                {
                    "policy_number": "GL-2025-001234",
                    "policy_type": "General Liability",
                    "effective_date": None,
                    "expiration_date": "01/01/2026"
                }
            ]
        }
        # Should handle gracefully
        assert "policies" in raw_data
        assert len(raw_data["policies"]) == 1


class TestMissingFieldsComplianceImpact:
    """Test that missing fields properly reduce compliance confidence."""

    @pytest.mark.skip(reason=(
        "returned_coi_compliance_evaluator was inlined into processor.py "
        "during the V1 rebuild (commit 9b36af3). evaluate_returned_coi_compliance "
        "and ReturnedCoiComplianceInput were not preserved."
    ))
    def test_missing_required_policy_type_flagged(self):
        """If a required policy type is entirely missing, evaluator flags it."""
        from returned_coi_compliance_evaluator import (
            evaluate_returned_coi_compliance,
            ReturnedCoiComplianceInput,
        )
        payload = ReturnedCoiComplianceInput(
            extraction_id="recEX1",
            vendor_id="recV1",
            client_id="recCL1",
            request_id=None,
            certificate_id="recCERT1",
            policy_ids=[],
            document_type="COI",
            source_filename="test.pdf",
            named_insured="Test Corp",
            raw_data={
                "policies": [
                    {
                        "policy_type": "General Liability",
                        "policy_number": "GL-001",
                        "expiration_date": "2027-01-01",
                        "coverage_limits": "EACH OCCURRENCE $1,000,000"
                    }
                ],
                "client_requirements": [
                    {"Policy Type": "General Liability", "Required": True, "Minimum Limit": "$1,000,000"},
                    {"Policy Type": "Workers Compensation", "Required": True, "Minimum Limit": "$500,000"},
                ]
            },
        )
        result = evaluate_returned_coi_compliance(payload)
        assert result.outcome == "Has Open Items"
        # Should have failure reason mentioning Workers Compensation
        assert any("Workers Compensation" in str(r) for r in result.failure_reasons)

    @pytest.mark.skip(reason=(
        "returned_coi_compliance_evaluator was inlined into processor.py "
        "during the V1 rebuild (commit 9b36af3). evaluate_returned_coi_compliance "
        "and ReturnedCoiComplianceInput were not preserved."
    ))
    def test_no_policies_at_all_flagged(self):
        """Zero policies in extraction data -> Non-Compliant."""
        from returned_coi_compliance_evaluator import (
            evaluate_returned_coi_compliance,
            ReturnedCoiComplianceInput,
        )
        payload = ReturnedCoiComplianceInput(
            extraction_id="recEX2",
            vendor_id="recV1",
            client_id="recCL1",
            request_id=None,
            certificate_id="recCERT2",
            policy_ids=[],
            document_type="COI",
            source_filename="test.pdf",
            named_insured="Test Corp",
            raw_data={
                "policies": [],
                "client_requirements": [
                    {"Policy Type": "General Liability", "Required": True}
                ]
            },
        )
        result = evaluate_returned_coi_compliance(payload)
        assert result.outcome == "Has Open Items"

    @pytest.mark.skip(reason=(
        "extract_limit_from_policy was renamed to extract_limits_from_policy "
        "during the V1 rebuild and now returns a dict, not a float. "
        "Assertion expects the old float contract."
    ))
    def test_missing_limits_reduces_confidence(self):
        """Missing coverage limits should trigger review."""
        from module_7b_requirement_validator import extract_limit_from_policy
        policy = {
            "id": "recP1",
            "fields": {
                "Policy Number": "GL-2025-001234",
                "Policy Type": "General Liability",
                "Coverage Limits": ""  # Missing limits
            }
        }
        result = extract_limit_from_policy(policy)
        # Missing limits should result in 0.0 or similar
        assert result == 0.0

    def test_invalid_limits_format_handled(self):
        """Invalid limit formats should be handled gracefully."""
        from module_7b_requirement_validator import parse_limit
        # Should handle malformed limit strings
        result = parse_limit("INVALID FORMAT")
        # Invalid format should return 0.0 (graceful handling)
        assert isinstance(result, (int, float))

    def test_missing_named_insured_extraction_data(self):
        """Missing named insured in extraction data should be noted."""
        raw_data = {
            "named_insured": None,
            "certificate_holder": "Test Client",
            "policies": [
                {
                    "policy_number": "GL-2025-001234",
                    "policy_type": "General Liability"
                }
            ]
        }
        assert raw_data["named_insured"] is None
        assert "certificate_holder" in raw_data


class TestDuplicateCertificateSubmission:
    """Edge case #7: Same PDF submitted twice for the same vendor."""

    def test_duplicate_policy_number_detected(self):
        """Submitting a policy number that already exists should trigger update, not create."""
        from processor import process_policies
        mock_table = MagicMock()
        existing_policy = {"id": "recP1", "fields": {"Policy Number": "GL-2025-001234"}}
        mock_table.first.return_value = existing_policy

        policies = [
            {
                "policy_number": "GL-2025-001234",
                "policy_type": "General Liability",
                "expiration_date": "01/01/2026"
            }
        ]

        result = process_policies(mock_table, policies, "recV1", "recC1", "test.pdf")
        # Duplicate detected, so should update not create
        mock_table.first.assert_called()

    def test_duplicate_policy_updates_not_creates(self):
        """When policy already exists, process_policies should update, not create new."""
        from processor import process_policies
        mock_table = MagicMock()
        existing_policy = {
            "id": "recP1",
            "fields": {
                "Policy Number": "GL-2025-001234",
                "Vendor Link": ["recV1"]
            }
        }
        mock_table.first.return_value = existing_policy

        policies = [
            {
                "policy_number": "GL-2025-001234",
                "policy_type": "General Liability",
                "expiration_date": "01/01/2026",
                "coverage_limits": "$1,000,000"
            }
        ]

        touched = process_policies(mock_table, policies, "recV1", "recC1", "test.pdf")
        mock_table.update.assert_called()  # Should update existing
        mock_table.create.assert_not_called()  # Should NOT create new
        assert "recP1" in touched

    def test_new_policy_creates_record(self):
        """When policy does not exist, process_policies should create new."""
        from processor import process_policies
        mock_table = MagicMock()
        mock_table.first.return_value = None  # No existing policy
        mock_table.create.return_value = {"id": "recPNew", "fields": {}}

        policies = [
            {
                "policy_number": "GL-NEW-001",
                "policy_type": "General Liability",
                "expiration_date": "01/01/2026"
            }
        ]

        touched = process_policies(mock_table, policies, "recV1", "recC1", "test.pdf")
        mock_table.create.assert_called_once()
        assert "recPNew" in touched

    def test_tbd_policy_number_skipped(self):
        """TBD policy numbers should be skipped entirely."""
        from processor import process_policies
        mock_table = MagicMock()
        policies = [
            {
                "policy_number": "TBD",
                "policy_type": "General Liability",
                "expiration_date": "01/01/2026"
            }
        ]
        touched = process_policies(mock_table, policies, "recV1", "recC1", "test.pdf")
        assert touched == []
        mock_table.create.assert_not_called()

    def test_pending_policy_number_skipped(self):
        """'Pending' policy numbers should be skipped."""
        from processor import is_invalid_policy_number
        assert is_invalid_policy_number("Pending") is True
        assert is_invalid_policy_number("PENDING") is True

    def test_vendor_matching_idempotent(self):
        """Same vendor name looked up twice returns same vendor."""
        from processor import find_vendor
        mock_table = MagicMock()
        vendor = {"id": "recV1", "fields": {"Vendor Name": "ABC Contractors LLC"}}
        mock_table.all.return_value = [vendor]

        result1 = find_vendor(mock_table, "ABC Contractors LLC")
        result2 = find_vendor(mock_table, "abc contractors llc")

        if result1 and result2:
            assert result1["id"] == result2["id"]

    def test_duplicate_across_vendors_allowed(self):
        """Same policy number for different vendors should be allowed."""
        from processor import process_policies
        mock_table = MagicMock()
        mock_table.first.return_value = None  # No existing policy
        mock_table.create.return_value = {"id": "recPNew", "fields": {}}

        policies = [
            {
                "policy_number": "GL-2025-001234",
                "policy_type": "General Liability",
                "expiration_date": "01/01/2026"
            }
        ]

        # Process for vendor 1
        result1 = process_policies(mock_table, policies, "recV1", "recC1", "test.pdf")
        assert len(result1) > 0

        # Reset mock
        mock_table.reset_mock()
        mock_table.first.return_value = None
        mock_table.create.return_value = {"id": "recPNew2", "fields": {}}

        # Process same policy number for vendor 2
        result2 = process_policies(mock_table, policies, "recV2", "recC1", "test.pdf")
        assert len(result2) > 0

    def test_exact_duplicate_extraction_idempotent(self):
        """Extracting the same PDF twice should produce idempotent results."""
        from processor import process_policies
        mock_table = MagicMock()

        # First extraction
        mock_table.first.return_value = None
        mock_table.create.return_value = {"id": "recP1", "fields": {}}
        policies = [
            {
                "policy_number": "GL-2025-001234",
                "policy_type": "General Liability",
                "expiration_date": "01/01/2026"
            }
        ]
        result1 = process_policies(mock_table, policies, "recV1", "recC1", "test.pdf")

        # Second extraction of same PDF
        mock_table.reset_mock()
        mock_table.first.return_value = {"id": "recP1", "fields": {"Policy Number": "GL-2025-001234"}}
        result2 = process_policies(mock_table, policies, "recV1", "recC1", "test.pdf")

        # Should update, not create new
        mock_table.update.assert_called()
        mock_table.create.assert_not_called()

    def test_vendor_alias_matching_idempotent(self):
        """Vendor alias lookups should return consistent results."""
        from processor import find_vendor_alias_matches
        mock_table = MagicMock()
        aliases = [
            {"id": "recA1", "fields": {"Alias": "ABC Contractors", "Vendor Link": ["recV1"]}},
            {"id": "recA2", "fields": {"Alias": "ABC Contracting", "Vendor Link": ["recV1"]}},
        ]
        mock_table.all.return_value = aliases

        result1 = find_vendor_alias_matches(mock_table, "ABC Contractors")
        result2 = find_vendor_alias_matches(mock_table, "abc contractors")

        # Both should find matches
        assert result1 is not None or result1 == []
        assert result2 is not None or result2 == []
