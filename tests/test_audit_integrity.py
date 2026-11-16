"""Tests for audit_buckets.compute_integrity_checks()."""
import pytest
from audit_buckets import compute_bucket_summary, compute_integrity_checks


def _make_cls(payee_raw, payee_group, vendor_id=""):
    return {
        "payee_raw":   payee_raw,
        "payee_group": payee_group,
        "vendor_id":   vendor_id,
    }


def _make_sub(payee_raw, status, total_paid, vendor_id=""):
    return {
        "payee_raw":  payee_raw,
        "status":     status,
        "total_paid": total_paid,
        "vendor_id":  vendor_id,
    }


def _make_pay(amount):
    return {"total_amount": amount}


def _run(cls, subs, pay, file_total):
    bs = compute_bucket_summary(subs, cls, pay, file_total)
    return compute_integrity_checks(bs, cls, pay, subs, file_total)


# ── Check 1: file total matches ───────────────────────────────────────────────

def test_file_total_matches_pass():
    cls  = [_make_cls("Sub A", "ccs_vendor", "v1")]
    subs = [_make_sub("Sub A", "full", 50000, "v1")]
    pay  = {"Sub A": _make_pay(50000)}
    checks = _run(cls, subs, pay, file_total=50000)
    c1 = next(c for c in checks if c["check"] == "File total matches")
    assert c1["pass"] is True


def test_file_total_mismatch_fails():
    cls  = [_make_cls("Sub A", "ccs_vendor", "v1")]
    subs = [_make_sub("Sub A", "full", 50000, "v1")]
    pay  = {"Sub A": _make_pay(50000)}
    checks = _run(cls, subs, pay, file_total=49000)
    c1 = next(c for c in checks if c["check"] == "File total matches")
    assert c1["pass"] is False
    assert "1,000" in c1["detail"] or "1000" in c1["detail"]


# ── Check 2: all payees classified ────────────────────────────────────────────

def test_all_payees_classified_pass():
    cls  = [_make_cls("Sub A", "ccs_vendor"), _make_cls("Supplier B", "supplier")]
    pay  = {"Sub A": _make_pay(10000), "Supplier B": _make_pay(5000)}
    checks = _run(cls, [], pay, file_total=15000)
    c2 = next(c for c in checks if c["check"] == "All payees classified")
    assert c2["pass"] is True


def test_unclassified_payee_fails():
    cls  = [_make_cls("Sub A", "ccs_vendor")]
    pay  = {"Sub A": _make_pay(10000), "Ghost Inc": _make_pay(3000)}
    checks = _run(cls, [], pay, file_total=13000)
    c2 = next(c for c in checks if c["check"] == "All payees classified")
    assert c2["pass"] is False
    assert "Ghost" in c2["detail"] or "1 payee" in c2["detail"]


# ── Check 3: payee counts match ───────────────────────────────────────────────

def test_payee_counts_match_pass():
    cls  = [_make_cls("Sub A", "ccs_vendor"), _make_cls("Supplier B", "supplier")]
    pay  = {"Sub A": _make_pay(10000), "Supplier B": _make_pay(5000)}
    checks = _run(cls, [], pay, file_total=15000)
    c3 = next(c for c in checks if c["check"] == "Payee counts match")
    assert c3["pass"] is True


def test_payee_counts_mismatch_fails():
    # Three payees in classifications, two in file
    cls  = [
        _make_cls("Sub A", "ccs_vendor"),
        _make_cls("Sub B", "ccs_vendor"),
        _make_cls("Sub C", "likely_sub"),  # not in file
    ]
    pay  = {"Sub A": _make_pay(10000), "Sub B": _make_pay(8000)}
    checks = _run(cls, [], pay, file_total=18000)
    c3 = next(c for c in checks if c["check"] == "Payee counts match")
    assert c3["pass"] is False


# ── Check 4: vendor totals consistent ────────────────────────────────────────

def test_vendor_totals_consistent_pass():
    cls  = [_make_cls("Sub A", "ccs_vendor", "v1")]
    subs = [_make_sub("Sub A", "full", 50000, "v1")]
    pay  = {"Sub A": _make_pay(50000)}
    checks = _run(cls, subs, pay, file_total=50000)
    c4 = next(c for c in checks if c["check"] == "Vendor totals consistent")
    assert c4["pass"] is True


def test_vendor_totals_over_limit_fails():
    # Analysis says vendor was paid 60K but file only has 50K for that payee
    cls  = [_make_cls("Sub A", "ccs_vendor", "v1")]
    subs = [_make_sub("Sub A", "full", 60000, "v1")]  # 60K in analysis
    pay  = {"Sub A": _make_pay(50000)}                # but only 50K in file
    checks = _run(cls, subs, pay, file_total=50000)
    c4 = next(c for c in checks if c["check"] == "Vendor totals consistent")
    assert c4["pass"] is False


# ── Full happy path ───────────────────────────────────────────────────────────

def test_all_checks_pass_happy_path():
    cls  = [
        _make_cls("Sub A", "ccs_vendor", "v1"),
        _make_cls("Sub B", "ccs_vendor", "v2"),
        _make_cls("HD Supply", "supplier"),
    ]
    subs = [
        _make_sub("Sub A", "full", 50000, "v1"),
        _make_sub("Sub B", "gap",  30000, "v2"),
    ]
    pay  = {
        "Sub A":     _make_pay(50000),
        "Sub B":     _make_pay(30000),
        "HD Supply": _make_pay(5000),
    }
    checks = _run(cls, subs, pay, file_total=85000)
    for c in checks:
        assert c["pass"] is True, f"Expected {c['check']} to pass: {c['detail']}"
