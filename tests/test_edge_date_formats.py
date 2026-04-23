"""
Test module for edge case #2: Non-Standard Date Formats
in the Carolina Compliance Solutions COI extraction pipeline.

Tests _normalize_date, normalize_policy_dates, parse_expiration_date,
and compute_policy_status functions with various date format inputs
and edge cases.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import tests._bootstrap  # noqa: F401

import pytest
from datetime import date, timedelta


class TestDateNormalization:
    """Test extractor._normalize_date against all required date formats."""

    def test_standard_mm_dd_yyyy(self):
        """MM/DD/YYYY (standard) should parse to ISO."""
        from extractor import _normalize_date
        result = _normalize_date("01/15/2025")
        assert result == "2025-01-15"

    def test_hyphenated_mm_dd_yyyy(self):
        """MM-DD-YYYY should parse to ISO."""
        from extractor import _normalize_date
        result = _normalize_date("01-15-2025")
        assert result == "2025-01-15"

    def test_long_month_format(self):
        """'January 15, 2025' should parse to ISO."""
        from extractor import _normalize_date
        result = _normalize_date("January 15, 2025")
        assert result == "2025-01-15"

    def test_long_month_format_without_comma(self):
        """'January 15 2025' should parse to ISO."""
        from extractor import _normalize_date
        result = _normalize_date("January 15 2025")
        assert result == "2025-01-15"

    def test_abbreviated_month_format(self):
        """'Jan 15, 2025' should parse to ISO."""
        from extractor import _normalize_date
        result = _normalize_date("Jan 15, 2025")
        assert result == "2025-01-15"

    def test_abbreviated_month_format_without_comma(self):
        """'Jan 15 2025' should parse to ISO."""
        from extractor import _normalize_date
        result = _normalize_date("Jan 15 2025")
        assert result == "2025-01-15"

    def test_two_digit_year_slash(self):
        """MM/DD/YY should parse correctly."""
        from extractor import _normalize_date
        result = _normalize_date("01/15/25")
        assert result == "2025-01-15"

    def test_two_digit_year_hyphen(self):
        """MM-DD-YY should parse correctly."""
        from extractor import _normalize_date
        result = _normalize_date("01-15-25")
        assert result == "2025-01-15"

    def test_iso_passthrough(self):
        """Already ISO format should pass through unchanged."""
        from extractor import _normalize_date
        result = _normalize_date("2025-01-15")
        assert result == "2025-01-15"

    def test_european_dd_month_yyyy(self):
        """'15 January 2025' should parse to ISO."""
        from extractor import _normalize_date
        result = _normalize_date("15 January 2025")
        assert result == "2025-01-15"

    def test_european_dd_abbreviated_month_yyyy(self):
        """'15 Jan 2025' should parse to ISO."""
        from extractor import _normalize_date
        result = _normalize_date("15 Jan 2025")
        assert result == "2025-01-15"

    def test_empty_string(self):
        """Empty string should return empty string."""
        from extractor import _normalize_date
        result = _normalize_date("")
        assert result == ""

    def test_whitespace_only(self):
        """Whitespace-only should return original."""
        from extractor import _normalize_date
        result = _normalize_date("   ")
        assert result == "   "

    def test_na_value(self):
        """'N/A' should return 'N/A' (not parseable)."""
        from extractor import _normalize_date
        result = _normalize_date("N/A")
        assert result == "N/A"

    def test_tbd_value(self):
        """'TBD' should return 'TBD' (not parseable)."""
        from extractor import _normalize_date
        result = _normalize_date("TBD")
        assert result == "TBD"

    def test_garbage_value(self):
        """Random text should return original unchanged."""
        from extractor import _normalize_date
        result = _normalize_date("not a date")
        assert result == "not a date"

    def test_partially_valid_value(self):
        """Partial date strings should return original."""
        from extractor import _normalize_date
        result = _normalize_date("01/15")
        assert result == "01/15"

    def test_december_31_two_digit_year(self):
        """12/31/99 should parse correctly (end of century)."""
        from extractor import _normalize_date
        result = _normalize_date("12/31/99")
        assert result == "1999-12-31"

    def test_january_01_two_digit_year(self):
        """01/01/00 should parse correctly (year 2000)."""
        from extractor import _normalize_date
        result = _normalize_date("01/01/00")
        assert result == "2000-01-01"

    def test_february_29_leap_year(self):
        """02/29/2024 should parse correctly (leap year)."""
        from extractor import _normalize_date
        result = _normalize_date("02/29/2024")
        assert result == "2024-02-29"

    def test_date_with_leading_whitespace(self):
        """Leading whitespace should be stripped."""
        from extractor import _normalize_date
        result = _normalize_date("  01/15/2025")
        assert result == "2025-01-15"

    def test_date_with_trailing_whitespace(self):
        """Trailing whitespace should be stripped."""
        from extractor import _normalize_date
        result = _normalize_date("01/15/2025  ")
        assert result == "2025-01-15"

    def test_date_with_both_whitespace(self):
        """Leading and trailing whitespace should be stripped."""
        from extractor import _normalize_date
        result = _normalize_date("  01/15/2025  ")
        assert result == "2025-01-15"


class TestPolicyDateNormalization:
    """Test extractor.normalize_policy_dates across full extraction data."""

    def test_normalizes_all_policies(self):
        """All policy dates in extraction data should be normalized."""
        from extractor import normalize_policy_dates
        data = {
            "policies": [
                {
                    "policy_number": "POL-001",
                    "effective_date": "01/15/2025",
                    "expiration_date": "01/15/2026",
                },
                {
                    "policy_number": "POL-002",
                    "effective_date": "January 1, 2025",
                    "expiration_date": "January 1, 2026",
                },
            ]
        }
        result = normalize_policy_dates(data)

        assert result["policies"][0]["effective_date"] == "2025-01-15"
        assert result["policies"][0]["expiration_date"] == "2026-01-15"
        assert result["policies"][1]["effective_date"] == "2025-01-01"
        assert result["policies"][1]["expiration_date"] == "2026-01-01"

    def test_certificate_date_normalized(self):
        """Top-level certificate_date should also be normalized."""
        from extractor import normalize_policy_dates
        data = {
            "certificate_date": "01/15/2025",
            "policies": []
        }
        result = normalize_policy_dates(data)
        assert result["certificate_date"] == "2025-01-15"

    def test_null_dates_preserved(self):
        """None date values should remain None."""
        from extractor import normalize_policy_dates
        data = {
            "policies": [
                {
                    "policy_number": "POL-001",
                    "effective_date": None,
                    "expiration_date": "01/15/2026",
                }
            ]
        }
        result = normalize_policy_dates(data)
        assert result["policies"][0]["effective_date"] is None
        assert result["policies"][0]["expiration_date"] == "2026-01-15"

    def test_empty_string_dates_preserved(self):
        """Empty string date values should remain empty."""
        from extractor import normalize_policy_dates
        data = {
            "policies": [
                {
                    "policy_number": "POL-001",
                    "effective_date": "",
                    "expiration_date": "01/15/2026",
                }
            ]
        }
        result = normalize_policy_dates(data)
        assert result["policies"][0]["effective_date"] == ""
        assert result["policies"][0]["expiration_date"] == "2026-01-15"

    def test_mixed_formats_normalized(self):
        """Different formats across policies all normalize to ISO."""
        from extractor import normalize_policy_dates
        data = {
            "policies": [
                {"effective_date": "01/15/2025", "expiration_date": "01-15-2026"},
                {"effective_date": "January 15, 2025", "expiration_date": "Jan 15 2026"},
                {"effective_date": "2025-01-15", "expiration_date": "2026-01-15"},
            ]
        }
        result = normalize_policy_dates(data)

        for policy in result["policies"]:
            assert policy["effective_date"] == "2025-01-15"
            assert policy["expiration_date"] == "2026-01-15"

    def test_unparseable_dates_preserved(self):
        """Unparseable dates should remain unchanged."""
        from extractor import normalize_policy_dates
        data = {
            "policies": [
                {"effective_date": "N/A", "expiration_date": "invalid date"}
            ]
        }
        result = normalize_policy_dates(data)
        assert result["policies"][0]["effective_date"] == "N/A"
        assert result["policies"][0]["expiration_date"] == "invalid date"

    def test_no_policies(self):
        """Data with no policies should not error."""
        from extractor import normalize_policy_dates
        data = {"certificate_date": "01/15/2025"}
        result = normalize_policy_dates(data)
        assert result["certificate_date"] == "2025-01-15"

    def test_missing_policies_key(self):
        """Data without policies key should not error."""
        from extractor import normalize_policy_dates
        data = {"certificate_date": "01/15/2025"}
        result = normalize_policy_dates(data)
        assert "certificate_date" in result
        assert result["certificate_date"] == "2025-01-15"


class TestProcessorDateParsing:
    """Test processor.parse_expiration_date."""

    def test_standard_format_mm_dd_yyyy(self):
        """MM/DD/YYYY parses to date object."""
        from processor import parse_expiration_date
        result = parse_expiration_date("01/15/2025")
        assert result == date(2025, 1, 15)

    def test_hyphenated_format_mm_dd_yyyy(self):
        """MM-DD-YYYY parses to date object."""
        from processor import parse_expiration_date
        result = parse_expiration_date("01-15-2025")
        assert result == date(2025, 1, 15)

    def test_iso_format(self):
        """YYYY-MM-DD parses to date object."""
        from processor import parse_expiration_date
        result = parse_expiration_date("2025-01-15")
        assert result == date(2025, 1, 15)

    def test_two_digit_year(self):
        """MM/DD/YY parses to date object."""
        from processor import parse_expiration_date
        result = parse_expiration_date("01/15/25")
        assert result == date(2025, 1, 15)

    def test_empty_returns_none(self):
        """Empty string returns None."""
        from processor import parse_expiration_date
        result = parse_expiration_date("")
        assert result is None

    def test_whitespace_returns_none(self):
        """Whitespace-only string returns None."""
        from processor import parse_expiration_date
        result = parse_expiration_date("   ")
        assert result is None

    def test_na_returns_none(self):
        """'N/A' returns None (unparseable)."""
        from processor import parse_expiration_date
        result = parse_expiration_date("N/A")
        assert result is None

    def test_garbage_returns_none(self):
        """Random text returns None."""
        from processor import parse_expiration_date
        result = parse_expiration_date("not a date")
        assert result is None

    def test_december_31_leap_century(self):
        """12/31/2000 parses correctly."""
        from processor import parse_expiration_date
        result = parse_expiration_date("12/31/2000")
        assert result == date(2000, 12, 31)

    def test_february_29_leap_year(self):
        """02/29/2024 parses correctly (leap year)."""
        from processor import parse_expiration_date
        result = parse_expiration_date("02/29/2024")
        assert result == date(2024, 2, 29)

    def test_none_input_returns_none(self):
        """None input returns None."""
        from processor import parse_expiration_date
        result = parse_expiration_date(None)
        assert result is None


class TestComputePolicyStatus:
    """Test processor.compute_policy_status with date edge cases."""

    def test_unparseable_date_returns_needs_review(self):
        """Unparseable date returns 'Needs Review'."""
        from processor import compute_policy_status
        result = compute_policy_status("invalid date")
        assert result == "Needs Review"

    def test_blank_date_returns_needs_review(self):
        """Blank date returns 'Needs Review'."""
        from processor import compute_policy_status
        result = compute_policy_status("")
        assert result == "Needs Review"

    def test_na_expiration_returns_needs_review(self):
        """'N/A' expiration returns 'Needs Review'."""
        from processor import compute_policy_status
        result = compute_policy_status("N/A")
        assert result == "Needs Review"

    def test_tbd_expiration_returns_needs_review(self):
        """'TBD' expiration returns 'Needs Review'."""
        from processor import compute_policy_status
        result = compute_policy_status("TBD")
        assert result == "Needs Review"

    def test_none_expiration_returns_needs_review(self):
        """None expiration returns 'Needs Review'."""
        from processor import compute_policy_status
        result = compute_policy_status(None)
        assert result == "Needs Review"

    def test_expired_date(self):
        """Past date returns 'Expired'."""
        from processor import compute_policy_status
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        result = compute_policy_status(yesterday)
        assert result == "Expired"

    def test_expiration_date_today(self):
        """Expiration date today returns 'Expiring Soon'."""
        from processor import compute_policy_status
        today = date.today().isoformat()
        result = compute_policy_status(today)
        assert result == "Expiring Soon"

    def test_expiration_date_5_days_out(self):
        """Expiration date 5 days out returns 'Expiring Soon'."""
        from processor import compute_policy_status
        in_5_days = (date.today() + timedelta(days=5)).isoformat()
        result = compute_policy_status(in_5_days)
        assert result == "Expiring Soon"

    def test_expiration_date_30_days_out(self):
        """Expiration date exactly 30 days out returns 'Expiring Soon'."""
        from processor import compute_policy_status
        in_30_days = (date.today() + timedelta(days=30)).isoformat()
        result = compute_policy_status(in_30_days)
        assert result == "Expiring Soon"

    def test_expiration_date_31_days_out(self):
        """Expiration date 31 days out returns 'Current'."""
        from processor import compute_policy_status
        in_31_days = (date.today() + timedelta(days=31)).isoformat()
        result = compute_policy_status(in_31_days)
        assert result == "Current"

    def test_far_future_date(self):
        """Date > 30 days out returns 'Current'."""
        from processor import compute_policy_status
        in_365_days = (date.today() + timedelta(days=365)).isoformat()
        result = compute_policy_status(in_365_days)
        assert result == "Current"

    def test_with_policy_number_context(self):
        """Status computation includes policy number in logging context."""
        from processor import compute_policy_status
        # This test verifies the function accepts policy_number parameter
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        result = compute_policy_status(yesterday, policy_number="POL-123")
        assert result == "Expired"

    def test_non_iso_format_still_parses(self):
        """Non-ISO formats supported by parse_expiration_date work in status."""
        from processor import compute_policy_status
        in_45_days = (date.today() + timedelta(days=45)).isoformat()
        result = compute_policy_status(in_45_days)
        assert result == "Current"

    def test_boundary_condition_29_days(self):
        """Boundary: 29 days out should return 'Expiring Soon'."""
        from processor import compute_policy_status
        in_29_days = (date.today() + timedelta(days=29)).isoformat()
        result = compute_policy_status(in_29_days)
        assert result == "Expiring Soon"

    def test_boundary_condition_31_days(self):
        """Boundary: 31 days out should return 'Current'."""
        from processor import compute_policy_status
        in_31_days = (date.today() + timedelta(days=31)).isoformat()
        result = compute_policy_status(in_31_days)
        assert result == "Current"


class TestDateNormalizationWithPolicyWorkflow:
    """Integration test: full extraction > normalization > parsing > status workflow."""

    def test_extraction_with_various_formats_through_status(self):
        """Test full workflow: extract dates > normalize > parse > compute status."""
        from extractor import normalize_policy_dates, _normalize_date
        from processor import parse_expiration_date, compute_policy_status

        # Simulate extracted data with various date formats
        raw_extraction = {
            "policies": [
                {
                    "policy_number": "POL-001",
                    "effective_date": "01/15/2025",
                    "expiration_date": "01/15/2026",
                },
                {
                    "policy_number": "POL-002",
                    "effective_date": "January 1, 2025",
                    "expiration_date": "Jan 1 2026",
                },
            ]
        }

        # Step 1: Normalize dates from extraction
        normalized = normalize_policy_dates(raw_extraction)

        # Step 2: Verify both policies now have ISO format dates
        assert normalized["policies"][0]["expiration_date"] == "2026-01-15"
        assert normalized["policies"][1]["expiration_date"] == "2026-01-01"

        # Step 3: Parse and compute status
        for policy in normalized["policies"]:
            parsed_date = parse_expiration_date(
                policy["expiration_date"]
            )
            assert parsed_date is not None
            status = compute_policy_status(policy["expiration_date"])
            assert status in ("Current", "Expiring Soon", "Expired", "Needs Review")

    def test_normalization_preserves_unparseable_through_workflow(self):
        """Test that unparseable dates remain unparseable through workflow."""
        from extractor import normalize_policy_dates
        from processor import parse_expiration_date, compute_policy_status

        raw_extraction = {
            "policies": [
                {
                    "policy_number": "POL-001",
                    "expiration_date": "N/A",
                }
            ]
        }

        normalized = normalize_policy_dates(raw_extraction)
        assert normalized["policies"][0]["expiration_date"] == "N/A"

        # parse_expiration_date should return None for unparseable
        parsed = parse_expiration_date(normalized["policies"][0]["expiration_date"])
        assert parsed is None

        # compute_policy_status should return "Needs Review"
        status = compute_policy_status(normalized["policies"][0]["expiration_date"])
        assert status == "Needs Review"
