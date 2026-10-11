import json
from pathlib import Path

import eval_scoring as es

GOLD = json.loads((Path(__file__).parent / "gold" / "rtr_gold.json").read_text())["pages"]


def perfect_output(page):
    names = {"GL": "COMMERCIAL GENERAL LIABILITY", "AUTO": "AUTOMOBILE LIABILITY",
             "WC": "WORKERS COMPENSATION AND EMPLOYERS' LIABILITY", "EXCESS_WC": "Excess Workers Compensation"}
    return {"document_type": page["doc_type"], "policies": [{
        "policy_type": names[p["family"]], "policy_number": p["number"], "carrier": "X",
        "effective_date": p["eff"], "expiration_date": p["exp"], "policy_basis": p["basis"],
        "additional_insured_checked": p["ai"], "waiver_of_subrogation_checked": p["wos"],
        "primary_noncontributory_checked": p["pnc"]} for p in page["policies"]]}


def test_gold_has_14_pages_and_perfect_output_scores_100():
    assert len(GOLD) == 14
    for pid, page in GOLD.items():
        s = es.score_page(page, perfect_output(page))
        assert s["errors"] == [], pid


def test_claims_made_error_is_caught():
    page = GOLD["f1_07"]
    out = perfect_output(page)
    out["policies"][0]["policy_basis"] = "claims-made"
    s = es.score_page(page, out)
    assert s["errors"] == ["GL.basis: expected 'occurrence', got 'claims-made'"]


def test_empty_policy_rows_are_ignored_but_wrong_count_is_not():
    page = GOLD["f1_04"]
    out = perfect_output(page)
    out["policies"].append({"policy_type": "UMBRELLA LIAB", "policy_number": None, "carrier": None})
    assert es.score_page(page, out)["errors"] == []
    out["policies"].append({"policy_type": "AUTOMOBILE LIABILITY", "policy_number": "Z1", "carrier": "Y"})
    assert any(e.startswith("policy_count") for e in es.score_page(page, out)["errors"])


def test_missing_policy_and_formats():
    page = GOLD["f1_01"]
    out = perfect_output(page)
    out["policies"] = out["policies"][:1]
    errs = es.score_page(page, out)["errors"]
    assert any("WC.number" in e for e in errs)
    assert es.norm_date("07/29/2026") == "2026-07-29"
    assert es.norm_number("MWTB 313070-26") == "MWTB31307026"
    assert es.family("Excess Workers Compensation") == "EXCESS_WC"


def test_summary_counts_checkbox_errors():
    page = GOLD["f1_06"]
    out = perfect_output(page)
    out["policies"][0]["additional_insured_checked"] = True
    sm = es.summarize({"f1_06": es.score_page(page, out)})
    assert sm["checkbox_errors"]["ai"] == 1 and sm["pages_perfect"] == 0


def test_compare_extractions_flags_only_real_differences():
    a = perfect_output(GOLD["f1_07"])
    b = perfect_output(GOLD["f1_07"])
    assert es.compare_extractions(a, b) == []
    b["policies"][0]["policy_basis"] = "claims-made"
    b["policies"].append({"policy_type": "UMBRELLA LIAB"})  # empty row ignored
    assert es.compare_extractions(a, b) == ["GL.basis: 'occurrence' vs 'claims-made'"]


def test_score_page_tristate_not_applicable_not_same_as_checked():
    """not_applicable gold entry does not match checked extraction."""
    page = dict(GOLD["f1_04"])  # single GL policy
    page = {**page, "policies": [{**page["policies"][0], "ai": "not_applicable"}]}
    out = perfect_output(GOLD["f1_04"])
    out["policies"][0]["additional_insured_checked"] = "checked"
    s = es.score_page(page, out)
    assert any("ai" in e for e in s["errors"])


def test_score_page_tristate_not_applicable_matches():
    """not_applicable gold entry matches not_applicable extraction."""
    page = dict(GOLD["f1_04"])
    page = {**page, "policies": [{**page["policies"][0], "ai": "not_applicable"}]}
    out = perfect_output(GOLD["f1_04"])
    out["policies"][0]["additional_insured_checked"] = "not_applicable"
    s = es.score_page(page, out)
    assert not any("ai" in e for e in s["errors"])


def test_compare_extractions_tristate_diff():
    """compare_extractions detects not_checked vs not_applicable as a real difference."""
    a = perfect_output(GOLD["f1_04"])
    b = perfect_output(GOLD["f1_04"])
    a["policies"][0]["additional_insured_checked"] = "not_checked"
    b["policies"][0]["additional_insured_checked"] = "not_applicable"
    diffs = es.compare_extractions(a, b)
    assert any("ai" in d for d in diffs)
