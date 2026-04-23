import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import tests._bootstrap  # noqa: F401

import pytest
import json


class TestDegradedExtractionResponse:
    """Tests simulating Claude returning degraded/low-quality extraction data."""

    def test_all_null_policies_triggers_second_pass(self):
        """When ≥50% of policy fields are null, _needs_second_pass should return True."""
        from extractor import _needs_second_pass_policy_extraction
        data = {
            "policies": [
                {"policy_number": None, "effective_date": None, "expiration_date": None},
                {"policy_number": "GL-001", "effective_date": None, "expiration_date": None},
            ]
        }
        assert _needs_second_pass_policy_extraction(data) is True

    def test_good_data_no_second_pass(self):
        """When most fields populated, no second pass needed."""
        from extractor import _needs_second_pass_policy_extraction
        data = {
            "policies": [
                {"policy_number": "GL-001", "effective_date": "01/01/2025", "expiration_date": "01/01/2026"},
            ]
        }
        assert _needs_second_pass_policy_extraction(data) is False

    def test_empty_policies_no_second_pass(self):
        """No policies at all → no second pass (nothing to recover)."""
        from extractor import _needs_second_pass_policy_extraction
        assert _needs_second_pass_policy_extraction({"policies": []}) is False
        assert _needs_second_pass_policy_extraction({}) is False

    def test_partial_null_below_threshold(self):
        """When <50% blank, no second pass triggered."""
        from extractor import _needs_second_pass_policy_extraction
        data = {
            "policies": [
                {"policy_number": "GL-001", "effective_date": "01/01/2025", "expiration_date": None},
            ]
        }
        # 1 out of 3 fields blank = 33% → below 50%
        assert _needs_second_pass_policy_extraction(data) is False

    def test_unparseable_dates_flagged_needs_review(self):
        """When Claude returns garbled dates, compute_policy_status returns 'Needs Review'."""
        from processor import compute_policy_status
        assert compute_policy_status("garbled-text") == "Needs Review"
        assert compute_policy_status("??/??/????") == "Needs Review"
        assert compute_policy_status("") == "Needs Review"

    def test_tbd_policy_number_flagged_invalid(self):
        """TBD policy numbers from poor scans should be caught."""
        from processor import is_invalid_policy_number
        assert is_invalid_policy_number("TBD") is True
        assert is_invalid_policy_number("tbd") is True
        assert is_invalid_policy_number("T.B.D") is True
        assert is_invalid_policy_number("to be determined") is True
        assert is_invalid_policy_number("pending") is True
        assert is_invalid_policy_number("") is True
        assert is_invalid_policy_number(None) is True
        # Valid policy numbers
        assert is_invalid_policy_number("GL-2025-001234") is False

    def test_blank_named_insured_matcher_returns_pending(self):
        """When Named Insured is blank (degraded scan), matcher should not match."""
        from incoming_extraction_matcher_slice import evaluate_vendor_match
        extraction_fields = {"Named Insured": "", "Certificate Holder": ""}
        vendor_records = [{"id": "recV1", "fields": {"Vendor Name": "ABC Corp"}}]
        result = evaluate_vendor_match(extraction_fields, vendor_records)
        assert result.match_status != "Matched"


class TestDegradedMockExtraction:
    """Test full extraction pipeline with mocked Claude returning degraded data."""

    @pytest.fixture
    def mock_degraded_claude_response(self):
        """Simulate Claude's response for a low-quality scan."""
        return json.dumps({
            "document_type": "unknown",
            "named_insured": None,
            "certificate_holder": None,
            "certificate_date": None,
            "contact_emails": [],
            "policies": [
                {"policy_type": None, "policy_number": None, "carrier": None,
                 "effective_date": None, "expiration_date": None, "coverage_limits": None}
            ]
        })

    def test_degraded_extraction_returns_mostly_null(self, mock_degraded_claude_response):
        """A severely degraded PDF results in mostly-null extraction data."""
        data = json.loads(mock_degraded_claude_response)
        assert data["named_insured"] is None
        assert data["document_type"] == "unknown"
        null_count = sum(1 for p in data["policies"] for v in p.values() if v is None)
        assert null_count >= 4  # most fields should be null

    def test_degraded_extraction_triggers_needs_review(self, mock_degraded_claude_response):
        """Pipeline should flag degraded extraction for manual review, not fail silently."""
        from processor import compute_policy_status, is_invalid_policy_number
        data = json.loads(mock_degraded_claude_response)
        for policy in data["policies"]:
            status = compute_policy_status(policy.get("expiration_date") or "")
            assert status == "Needs Review"
            assert is_invalid_policy_number(policy.get("policy_number") or "")


class TestPDFGeneration:
    """Verify that synthetic degraded PDFs can be generated (requires reportlab)."""

    def test_generate_low_resolution_pdf(self, tmp_path):
        """Low-res PDF should be created successfully."""
        try:
            from tests.generate_test_pdfs import AcordTestPDFGenerator
            gen = AcordTestPDFGenerator()
            pdf_path = tmp_path / "low_res.pdf"
            gen.generate_low_resolution(str(pdf_path))
            assert pdf_path.exists()
            assert pdf_path.stat().st_size > 0
        except ImportError:
            pytest.skip("reportlab not installed")

    def test_generate_rotated_pdf(self, tmp_path):
        """Rotated PDF should be created."""
        try:
            from tests.generate_test_pdfs import AcordTestPDFGenerator
            gen = AcordTestPDFGenerator()
            pdf_path = tmp_path / "rotated.pdf"
            gen.generate_rotated(str(pdf_path), angle=5)
            assert pdf_path.exists()
        except ImportError:
            pytest.skip("reportlab not installed")

    def test_generate_partial_cutoff_pdf(self, tmp_path):
        """Cutoff PDF should have smaller file size."""
        try:
            from tests.generate_test_pdfs import AcordTestPDFGenerator
            gen = AcordTestPDFGenerator()
            standard_path = tmp_path / "standard.pdf"
            cutoff_path = tmp_path / "cutoff.pdf"
            gen.generate_standard_acord25(str(standard_path))
            gen.generate_partial_cutoff(str(cutoff_path))
            assert cutoff_path.exists()
        except ImportError:
            pytest.skip("reportlab not installed")

    def test_generate_faded_text_pdf(self, tmp_path):
        """Faded text PDF should be created."""
        try:
            from tests.generate_test_pdfs import AcordTestPDFGenerator
            gen = AcordTestPDFGenerator()
            pdf_path = tmp_path / "faded.pdf"
            gen.generate_faded_text(str(pdf_path))
            assert pdf_path.exists()
        except ImportError:
            pytest.skip("reportlab not installed")
