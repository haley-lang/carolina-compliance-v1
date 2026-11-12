"""Tests for compliance_guard.py."""
import pytest
from compliance_guard import check, assert_clean, GuardResult


# ── Banned words ──────────────────────────────────────────────────────────────

class TestBannedWords:
    def test_compliant_blocked(self):
        r = check("Your vendor is compliant with requirements.")
        assert not r.ok
        assert any("compliant" in f for f in r.flags)

    def test_verified_blocked(self):
        r = check("All subs are verified.")
        assert not r.ok
        assert any("verified" in f for f in r.flags)

    def test_covered_blocked(self):
        r = check("Make sure your team is covered.")
        assert not r.ok
        assert any("covered" in f for f in r.flags)

    def test_certified_blocked(self):
        r = check("We are certified to handle this.")
        assert not r.ok
        assert any("certified" in f for f in r.flags)

    def test_compliance_blocked_outside_company_name(self):
        r = check("This meets compliance standards.")
        assert not r.ok
        assert any("compliance" in f for f in r.flags)

    def test_company_name_allowed(self):
        r = check("Carolina Compliance Solutions helps GCs stay organized.")
        assert r.ok, f"Expected PASS but got flags: {r.flags}"

    def test_ccs_abbreviation_allowed(self):
        r = check("CCS makes the process simple.")
        assert r.ok

    def test_on_file_always_allowed(self):
        r = check("Your certificate is on file.")
        assert r.ok

    def test_not_on_file_always_allowed(self):
        r = check("That policy is not on file yet.")
        assert r.ok

    def test_multiple_banned_words_reported(self):
        r = check("vendor is compliant and verified.")
        assert not r.ok
        assert len(r.flags) >= 2

    def test_case_insensitive(self):
        assert not check("COMPLIANT").ok
        assert not check("Verified").ok
        assert not check("COVERED").ok


# ── Audit-claim patterns ──────────────────────────────────────────────────────

class TestAuditClaims:
    def test_auditors_require_all_blocked(self):
        r = check("Auditors require all policy terms going back five years.")
        assert not r.ok
        assert any("audit-claim" in f for f in r.flags)

    def test_auditors_review_past_year_blocked(self):
        r = check("Auditors review past year records during an audit.")
        assert not r.ok
        assert any("audit-claim" in f for f in r.flags)

    def test_auditors_ask_for_prior_blocked(self):
        r = check("Auditors ask for prior certificates of insurance.")
        assert not r.ok
        assert any("audit-claim" in f for f in r.flags)

    def test_percent_compliant_blocked(self):
        r = check("95% compliant across all vendors.")
        assert not r.ok

    def test_percent_compliant_text_blocked(self):
        r = check("Your percent compliant score improved.")
        assert not r.ok
        assert any("audit-claim" in f for f in r.flags)

    def test_audit_general_statement_ok(self):
        r = check("Being ready for an audit is easier when certificates are on file.")
        assert r.ok, f"Expected PASS but got flags: {r.flags}"

    def test_auditors_check_recent_ok(self):
        r = check("Auditors typically look at the most recent year's certificates.")
        assert r.ok, f"Expected PASS but got flags: {r.flags}"


# ── State-specific claims ─────────────────────────────────────────────────────

class TestStateClaims:
    def test_north_carolina_law_blocked(self):
        r = check("North Carolina law requires all GCs to hold certificates.")
        assert not r.ok
        assert any("state-claim" in f for f in r.flags)

    def test_state_law_requires_blocked(self):
        r = check("State law requires this documentation.")
        assert not r.ok
        assert any("state-claim" in f for f in r.flags)

    def test_general_audience_statement_ok(self):
        r = check("Most GCs we work with need GL and Workers Comp on file.")
        assert r.ok

    def test_florida_audit_blocked(self):
        r = check("Florida audit rules are stricter than other states.")
        assert not r.ok


# ── GuardResult interface ─────────────────────────────────────────────────────

class TestGuardResult:
    def test_ok_is_truthy(self):
        r = GuardResult(ok=True, flags=[])
        assert r
        assert bool(r) is True

    def test_fail_is_falsy(self):
        r = GuardResult(ok=False, flags=["banned-word:compliant"])
        assert not r
        assert bool(r) is False

    def test_repr_pass(self):
        r = GuardResult(ok=True, flags=[])
        assert "PASS" in repr(r)

    def test_repr_fail(self):
        r = GuardResult(ok=False, flags=["x"])
        assert "FAIL" in repr(r)


# ── assert_clean ─────────────────────────────────────────────────────────────

class TestAssertClean:
    def test_clean_text_returned_unchanged(self):
        text = "Certificates on file make audits smoother."
        assert assert_clean(text) == text

    def test_banned_word_raises(self):
        with pytest.raises(ValueError, match="compliance guard"):
            assert_clean("Your team is compliant.")

    def test_audit_claim_raises(self):
        with pytest.raises(ValueError):
            assert_clean("Auditors require all policy terms.")
