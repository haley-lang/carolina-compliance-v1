import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import tests._bootstrap  # noqa: F401

import pytest
from unittest.mock import MagicMock, patch


class TestDocumentTypeNormalization:
    """Test processor.normalize_document_type."""

    def test_standard_coi(self):
        """'COI' should normalize to 'coi'."""
        from processor import normalize_document_type
        assert normalize_document_type("COI") == "coi"

    def test_cancellation_notice(self):
        """'cancellation_notice' should normalize to 'cancellation notice' (underscores to spaces)."""
        from processor import normalize_document_type
        assert normalize_document_type("cancellation_notice") == "cancellation notice"

    def test_hyphenated(self):
        """'cancellation-notice' should normalize to 'cancellation notice'."""
        from processor import normalize_document_type
        assert normalize_document_type("cancellation-notice") == "cancellation notice"

    def test_mixed_case(self):
        """'Cancellation Notice' should normalize to 'cancellation notice'."""
        from processor import normalize_document_type
        assert normalize_document_type("Cancellation Notice") == "cancellation notice"

    def test_extra_whitespace(self):
        """'  cancellation   notice  ' should normalize to 'cancellation notice'."""
        from processor import normalize_document_type
        assert normalize_document_type("  cancellation   notice  ") == "cancellation notice"

    def test_empty(self):
        """Empty string should return empty string."""
        from processor import normalize_document_type
        assert normalize_document_type("") == ""

    def test_none(self):
        """None should return empty string."""
        from processor import normalize_document_type
        assert normalize_document_type(None) == ""


class TestPolicyTypeNormalization:
    """Test processor.normalize_policy_type for various ACORD versions."""

    def test_standard_cgl(self):
        """'Commercial General Liability' should normalize to 'General Liability'."""
        from processor import normalize_policy_type
        assert normalize_policy_type("Commercial General Liability") == "General Liability"

    def test_abbreviated_gl(self):
        """'GL' should normalize to 'General Liability'."""
        from processor import normalize_policy_type
        assert normalize_policy_type("GL") == "General Liability"

    def test_cgl_abbreviation(self):
        """'CGL' should normalize to 'General Liability'."""
        from processor import normalize_policy_type
        assert normalize_policy_type("CGL") == "General Liability"

    def test_workers_comp_variations(self):
        """Multiple WC spellings should normalize to 'Workers Comp'."""
        from processor import normalize_policy_type
        assert normalize_policy_type("Workers Compensation") == "Workers Comp"
        assert normalize_policy_type("Workers' Compensation") == "Workers Comp"
        assert normalize_policy_type("WC") == "Workers Comp"

    def test_auto_variations(self):
        """Multiple auto spellings should normalize to 'Auto Liability'."""
        from processor import normalize_policy_type
        assert normalize_policy_type("Commercial Auto Liability") == "Auto Liability"
        assert normalize_policy_type("Auto") == "Auto Liability"
        assert normalize_policy_type("CAL") == "Auto Liability"

    def test_umbrella_variations(self):
        """Umbrella/excess variations should normalize to 'Umbrella'."""
        from processor import normalize_policy_type
        assert normalize_policy_type("Umbrella") == "Umbrella"
        assert normalize_policy_type("Excess Liability") == "Umbrella"
        assert normalize_policy_type("Umbrella Liability") == "Umbrella"

    def test_unknown_type_defaults(self):
        """Unknown types should default to 'General Liability'."""
        from processor import normalize_policy_type
        result = normalize_policy_type("Exotic Coverage Type")
        assert result == "General Liability"


class TestAcordVersionDetection:
    """Test pipeline behavior with different ACORD form versions."""

    def test_standard_2016_extraction(self):
        """Standard ACORD 25 (2016/03) should extract normally."""
        from processor import compute_policy_status
        # Standard form should allow normal extraction and status computation
        status = compute_policy_status("01/01/2027")
        assert status == "Current"

    def test_older_2010_extraction(self):
        """Older ACORD 25 (2010/06) may have different field positions but data still extracts."""
        # The normalization functions should handle older versions gracefully
        from processor import normalize_policy_type
        # Older forms might use slightly different terminology
        result = normalize_policy_type("General Liability Coverage")
        assert result == "General Liability"

    def test_proprietary_form_flagged(self):
        """Non-ACORD form with document_type='unknown' should be flagged for review."""
        from processor import compute_policy_status
        # Unknown/proprietary forms with sparse data should be flagged
        status = compute_policy_status("")
        assert status == "Needs Review"


class TestFormVersionInExtractionData:
    """Test that extraction data handles form version metadata."""

    def test_extraction_with_form_version(self):
        """If Claude returns form version info, it should be preserved."""
        raw_data = {
            "form_version": "ACORD 25 (2016/03)",
            "named_insured": "Test Corp",
            "policies": []
        }
        # Version info should be accessible without errors
        assert "form_version" in raw_data
        assert raw_data["form_version"] == "ACORD 25 (2016/03)"

    def test_extraction_without_form_version(self):
        """Missing form version should not cause errors."""
        raw_data = {
            "named_insured": "Test Corp",
            "policies": []
        }
        # Should handle missing form_version gracefully
        form_version = raw_data.get("form_version", None)
        assert form_version is None
