"""Tests for A1 fix: GL keyword matching must not pick up Auto/Employers Liability.

Evidence:
- Christenbury Grading: GL was showing Auto Liability (Q09-1531691) dates instead of GL (Q61-0299209)
- Gomez Construction: GL was showing Auto Liability (Q12-1031003) dates instead of GL (Q61-0134820)
"""
import os
import pytest

os.environ.setdefault("REVIEW_PASSWORD", "test_review_pass")

from review_app import (
    _GL_KEYWORDS,
    _WC_KEYWORDS,
    _pol_matches_audit_type,
    _pol_type_abbrev,
)


# ── _pol_matches_audit_type ────────────────────────────────────────────────────

class TestPolMatchesAuditTypeGL:
    """GL keyword matching must exclude Auto and Employers Liability."""

    def test_commercial_general_liability_matches_gl(self):
        assert _pol_matches_audit_type("commercial general liability", _GL_KEYWORDS) is True

    def test_cgl_matches_gl(self):
        assert _pol_matches_audit_type("cgl", _GL_KEYWORDS) is True

    def test_general_liability_matches_gl(self):
        assert _pol_matches_audit_type("general liability", _GL_KEYWORDS) is True

    # --- Exclusions ---

    def test_auto_liability_does_not_match_gl(self):
        """Christenbury/Gomez root cause: 'auto liability' matches 'liability' keyword."""
        assert _pol_matches_audit_type("auto liability", _GL_KEYWORDS) is False

    def test_commercial_auto_liability_does_not_match_gl(self):
        assert _pol_matches_audit_type("commercial auto liability", _GL_KEYWORDS) is False

    def test_automobile_liability_does_not_match_gl(self):
        assert _pol_matches_audit_type("automobile liability", _GL_KEYWORDS) is False

    def test_employers_liability_does_not_match_gl(self):
        """Employers Liability is part of WC umbrella, not GL."""
        assert _pol_matches_audit_type("employers liability", _GL_KEYWORDS) is False

    def test_wc_and_employers_liability_does_not_match_gl(self):
        assert _pol_matches_audit_type("workers compensation / employers liability", _GL_KEYWORDS) is False


class TestPolMatchesAuditTypeWC:
    """WC keyword matching — no exclusions needed."""

    def test_workers_comp_matches(self):
        assert _pol_matches_audit_type("workers compensation", _WC_KEYWORDS) is True

    def test_workers_matches(self):
        assert _pol_matches_audit_type("workers", _WC_KEYWORDS) is True

    def test_wc_matches(self):
        assert _pol_matches_audit_type("wc", _WC_KEYWORDS) is True

    def test_employers_liability_matches_wc(self):
        """Employers Liability should match WC keywords."""
        assert _pol_matches_audit_type("employers liability", _WC_KEYWORDS) is True

    def test_auto_does_not_match_wc(self):
        assert _pol_matches_audit_type("auto liability", _WC_KEYWORDS) is False

    def test_gl_does_not_match_wc(self):
        assert _pol_matches_audit_type("commercial general liability", _WC_KEYWORDS) is False


# ── _pol_type_abbrev ───────────────────────────────────────────────────────────

class TestPolTypeAbbrev:
    """Auto check must come before GL check so 'Commercial Auto Liability' → 'Auto', not 'GL'."""

    def test_commercial_general_liability_is_gl(self):
        assert _pol_type_abbrev("Commercial General Liability") == "GL"

    def test_general_liability_is_gl(self):
        assert _pol_type_abbrev("General Liability") == "GL"

    def test_commercial_auto_liability_is_auto_not_gl(self):
        """Root cause fix: was returning 'GL' before, must now return 'Auto'."""
        assert _pol_type_abbrev("Commercial Auto Liability") == "Auto"

    def test_auto_liability_is_auto(self):
        assert _pol_type_abbrev("Auto Liability") == "Auto"

    def test_automobile_liability_is_auto(self):
        assert _pol_type_abbrev("Automobile Liability") == "Auto"

    def test_workers_comp_is_wc(self):
        assert _pol_type_abbrev("Workers Compensation") == "WC"

    def test_employers_liability_is_wc(self):
        """Employers Liability is WC umbrella — must not return GL."""
        result = _pol_type_abbrev("Employers Liability")
        assert result == "WC"

    def test_umbrella_is_umb(self):
        assert _pol_type_abbrev("Commercial Umbrella") == "Umb"


# ── Vendor-specific regression tests ──────────────────────────────────────────

class TestChristenburyGradingRegression:
    """Christenbury Grading: GL policy Q61-0299209 must not be shadowed by Auto Q09-1531691."""

    _POLICIES = [
        {
            "policy_type": "Commercial General Liability",
            "policy_number": "Q61-0299209",
            "effective_date": "2025-07-25",
            "expiration_date": "2026-07-25",
        },
        {
            "policy_type": "Commercial Auto Liability",
            "policy_number": "Q09-1531691",
            "effective_date": "2025-09-15",
            "expiration_date": "2026-09-15",
        },
    ]

    def test_gl_policy_matches_gl_keywords(self):
        pt = self._POLICIES[0]["policy_type"].lower()
        assert _pol_matches_audit_type(pt, _GL_KEYWORDS) is True

    def test_auto_policy_does_not_match_gl_keywords(self):
        pt = self._POLICIES[1]["policy_type"].lower()
        assert _pol_matches_audit_type(pt, _GL_KEYWORDS) is False

    def test_only_gl_policy_returned_for_gl_audit(self):
        gl_pols = [
            p for p in self._POLICIES
            if _pol_matches_audit_type(p["policy_type"].lower(), _GL_KEYWORDS)
        ]
        assert len(gl_pols) == 1
        assert gl_pols[0]["policy_number"] == "Q61-0299209"


class TestGomezConstructionRegression:
    """Gomez Construction: GL policy Q61-0134820 must not be shadowed by Auto Q12-1031003."""

    _POLICIES = [
        {
            "policy_type": "Commercial General Liability",
            "policy_number": "Q61-0134820",
            "effective_date": "2024-09-22",
            "expiration_date": "2025-09-22",
        },
        {
            "policy_type": "Auto Liability",
            "policy_number": "Q12-1031003",
            "effective_date": "2024-12-10",
            "expiration_date": "2025-12-10",
        },
    ]

    def test_auto_policy_does_not_match_gl_keywords(self):
        pt = self._POLICIES[1]["policy_type"].lower()
        assert _pol_matches_audit_type(pt, _GL_KEYWORDS) is False

    def test_only_gl_policy_returned_for_gl_audit(self):
        gl_pols = [
            p for p in self._POLICIES
            if _pol_matches_audit_type(p["policy_type"].lower(), _GL_KEYWORDS)
        ]
        assert len(gl_pols) == 1
        assert gl_pols[0]["policy_number"] == "Q61-0134820"


class TestWCVendorNotAffected:
    """WC vendor with Employers Liability — must match WC, not GL."""

    _POLICIES = [
        {
            "policy_type": "Workers Compensation and Employers Liability",
            "policy_number": "WC-2025-001",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
        },
    ]

    def test_wc_policy_matches_wc_keywords(self):
        pt = self._POLICIES[0]["policy_type"].lower()
        assert _pol_matches_audit_type(pt, _WC_KEYWORDS) is True

    def test_wc_policy_does_not_match_gl_keywords(self):
        """'Employers Liability' substring must be excluded from GL matching."""
        pt = self._POLICIES[0]["policy_type"].lower()
        assert _pol_matches_audit_type(pt, _GL_KEYWORDS) is False
