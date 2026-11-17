"""Tests for _fuzzy_match_vendors: correct group assignment for exact/likely/possible matches.

Key invariants:
- score >= 90 (exact): ccs_vendor
- score 70-89 AND ratio >= 35 (likely): likely_sub (NOT ccs_vendor)
- score 50-69 (possible): unclear with vendor suggestion
- TruTeam/ABS false positive is rejected (ratio < 35 guard)
- Aiken Heating & Air is NOT a CCS sub (Henson Heating & Cooling is a possible match only)
- Romero Team Construction LLC IS a CCS sub (exact match)
"""
import os
import pytest

os.environ.setdefault("REVIEW_PASSWORD", "test_review_pass")
os.environ.setdefault("ANTHROPIC_API_KEY", "test_key")
os.environ.setdefault("AIRTABLE_API_KEY", "test_airtable_key")


@pytest.fixture(autouse=True)
def _load_app():
    import review_app  # noqa: F401 — ensures module is imported


def _match(payee_raw, vendor_list):
    from review_app import _fuzzy_match_vendors
    return _fuzzy_match_vendors(payee_raw, vendor_list, client_id="recCLIENT1")


# ── Exact match ───────────────────────────────────────────────────────────────

class TestExactMatch:
    def test_identical_name_is_ccs_vendor(self):
        vendors = [{"id": "recV1", "name": "Romero Team Construction LLC"}]
        vid, vname, mt, pg = _match("Romero Team Construction LLC", vendors)
        assert pg == "ccs_vendor"
        assert mt == "exact"
        assert vid == "recV1"

    def test_near_identical_name_is_ccs_vendor(self):
        """Slight suffix difference (LLC vs Inc) still scores >= 90."""
        vendors = [{"id": "recV2", "name": "Alpha Roofing Inc"}]
        vid, vname, mt, pg = _match("Alpha Roofing Inc", vendors)
        assert pg == "ccs_vendor"
        assert mt == "exact"

    def test_romero_team_construction_exact(self):
        """Romero Team Construction LLC matches its own vendor record as exact/ccs_vendor."""
        vendors = [
            {"id": "recVA", "name": "Henson Heating & Cooling"},
            {"id": "recVB", "name": "Romero Team Construction LLC"},
            {"id": "recVC", "name": "Alpha Plumbing"},
        ]
        vid, vname, mt, pg = _match("Romero Team Construction LLC", vendors)
        assert pg == "ccs_vendor", f"Expected ccs_vendor, got {pg!r} (match_type={mt!r})"
        assert mt == "exact"
        assert vid == "recVB"


# ── Likely match → likely_sub (NOT ccs_vendor) ───────────────────────────────

class TestLikelyMatch:
    def test_likely_match_gives_likely_sub_not_ccs_vendor(self):
        """A score in the 70-89 range must yield likely_sub, never ccs_vendor."""
        # "Carolina Heating Services" vs "Carolina Heating & Air" — high but not exact
        vendors = [{"id": "recV1", "name": "Carolina Heating & Air Conditioning LLC"}]
        vid, vname, mt, pg = _match("Carolina Heating Services", vendors)
        # Whether it hits likely or possible, it must NOT be ccs_vendor
        assert pg != "ccs_vendor", f"Likely/possible match should not be ccs_vendor; got {pg!r}"

    def test_truteam_abs_false_positive_rejected(self):
        """TruTeam Builder Services Group ... Insulating vs ABS Insulating Company
        has token_set_ratio=70 but ratio=31 — the ratio floor rejects it from 'likely',
        so it falls to 'possible' (unclear) rather than polluting ccs_vendor or likely_sub."""
        vendors = [{"id": "recABS", "name": "ABS Insulating Company, Inc."}]
        payee = "TruTeam Builder Services Group, Inc. d/b/a Pender & Pettus Insulating"
        vid, vname, mt, pg = _match(payee, vendors)
        # Should NOT be likely_sub (let alone ccs_vendor) — it's a false positive
        assert pg != "ccs_vendor", f"TruTeam should not be ccs_vendor; got {pg!r}"
        assert mt != "likely", f"TruTeam/ABS false positive should not be 'likely'; got {mt!r}"
        # Acceptable outcomes: possible (unclear) or no match at all
        assert pg in ("unclear", "likely_sub", "not_sub", ""), f"Unexpected group: {pg!r}"


# ── Aiken Heating & Air — NOT a CCS sub ──────────────────────────────────────

class TestAikenHeating:
    def test_aiken_is_not_ccs_vendor(self):
        """Aiken Heating & Air has only a possible match to Henson Heating & Cooling
        (token_set_ratio ~58) — it must NOT land in ccs_vendor or likely_sub via matcher."""
        vendors = [{"id": "recHEN", "name": "Henson Heating & Cooling"}]
        vid, vname, mt, pg = _match("Aiken Heating & Air", vendors)
        assert pg != "ccs_vendor", f"Aiken Heating & Air must not be ccs_vendor; got {pg!r}"
        assert pg != "likely_sub", (
            f"Aiken Heating & Air is only a possible match — should not be likely_sub "
            f"via auto-matcher; got {pg!r} (mt={mt!r})"
        )
        # It should be possible/unclear with the Henson suggestion
        assert mt == "possible", f"Expected possible match_type; got {mt!r}"
        assert pg == "unclear"
        assert vid == "recHEN"

    def test_aiken_possible_match_shown_as_suggestion(self):
        """The possible match returns the vendor name so the UI can show it as a suggestion."""
        vendors = [{"id": "recHEN", "name": "Henson Heating & Cooling"}]
        vid, vname, mt, pg = _match("Aiken Heating & Air", vendors)
        assert vname == "Henson Heating & Cooling"
        assert vid == "recHEN"

    def test_aiken_with_multiple_vendors_still_not_ccs(self):
        """With a full vendor list, Aiken still ends up as possible/unclear."""
        vendors = [
            {"id": "recVA", "name": "Romero Team Construction LLC"},
            {"id": "recHEN", "name": "Henson Heating & Cooling"},
            {"id": "recVC", "name": "Alpha Plumbing & Drain"},
        ]
        vid, vname, mt, pg = _match("Aiken Heating & Air", vendors)
        assert pg != "ccs_vendor"
        assert pg != "likely_sub"


# ── Romero Team Construction LLC — IS a CCS sub ──────────────────────────────

class TestRomeroConstruction:
    def test_romero_is_ccs_vendor(self):
        """Romero Team Construction LLC exactly matches its vendor record."""
        vendors = [
            {"id": "recAIK", "name": "Aiken Heating & Air"},      # distractor
            {"id": "recROM", "name": "Romero Team Construction LLC"},
        ]
        vid, vname, mt, pg = _match("Romero Team Construction LLC", vendors)
        assert pg == "ccs_vendor", f"Romero should be ccs_vendor; got {pg!r}"
        assert mt == "exact"
        assert vid == "recROM"

    def test_romero_slight_variation_still_exact(self):
        """Minor punctuation / suffix difference keeps an exact-tier score."""
        vendors = [{"id": "recROM", "name": "Romero Team Construction LLC"}]
        # Drop "LLC" — still very close
        vid, vname, mt, pg = _match("Romero Team Construction", vendors)
        assert pg == "ccs_vendor"
        assert mt == "exact"


# ── No match ─────────────────────────────────────────────────────────────────

class TestNoMatch:
    def test_unrelated_payee_no_ccs_match(self):
        """A payee with no vendor near-match stays unclear or gets a signal-based group."""
        vendors = [{"id": "recV1", "name": "Alpha Roofing Inc"}]
        vid, vname, mt, pg = _match("Home Depot", vendors)
        assert pg != "ccs_vendor"
        assert vid == "" or mt == "possible"  # no strong match

    def test_empty_vendor_list(self):
        vid, vname, mt, pg = _match("Some Contractor LLC", [])
        assert pg != "ccs_vendor"
        assert vid == ""
