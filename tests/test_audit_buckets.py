"""Tests for audit_buckets: payee_bucket() and compute_bucket_summary()."""
import pytest
from audit_buckets import (
    payee_bucket,
    compute_bucket_summary,
    BUCKET_LABELS,
    BUCKET_ORDER,
)


# ── payee_bucket ──────────────────────────────────────────────────────────────

def test_ccs_vendor_full_is_sub_on_file():
    assert payee_bucket("ccs_vendor", "full") == "sub_on_file"


def test_ccs_vendor_gap_is_sub_with_gap():
    assert payee_bucket("ccs_vendor", "gap") == "sub_with_gap"


def test_ccs_vendor_short_gap_is_sub_with_gap():
    assert payee_bucket("ccs_vendor", "short_gap") == "sub_with_gap"


def test_ccs_vendor_not_in_ccs_is_sub_with_gap():
    # ccs_vendor but status = not_in_ccs means partial / unknown → sub_with_gap
    assert payee_bucket("ccs_vendor", "not_in_ccs") == "sub_with_gap"


def test_ccs_vendor_none_in_window_is_sub_with_gap():
    assert payee_bucket("ccs_vendor", "none_in_window") == "sub_with_gap"


def test_likely_sub_is_sub_no_ccs_record():
    assert payee_bucket("likely_sub") == "sub_no_ccs_record"


def test_likely_sub_ignores_status():
    assert payee_bucket("likely_sub", "full") == "sub_no_ccs_record"


def test_not_sub_is_supplier():
    # Legacy not_sub maps to supplier bucket
    assert payee_bucket("not_sub") == "supplier"


def test_supplier_is_supplier():
    assert payee_bucket("supplier") == "supplier"


def test_owner_payroll_is_own_bucket():
    assert payee_bucket("owner_payroll") == "owner_payroll"


def test_unclear_is_needs_decision():
    assert payee_bucket("unclear") == "needs_decision"


def test_unknown_group_is_needs_decision():
    assert payee_bucket("") == "needs_decision"
    assert payee_bucket("whatever") == "needs_decision"


# ── compute_bucket_summary ────────────────────────────────────────────────────

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


def _make_pay(payee_raw, amount):
    return {"total_amount": amount}


def test_full_ccs_vendor_goes_to_sub_on_file():
    cls  = [_make_cls("Alpha LLC", "ccs_vendor", "v1")]
    subs = [_make_sub("Alpha LLC", "full", 50000)]
    pay  = {"Alpha LLC": _make_pay("Alpha LLC", 50000)}

    result = compute_bucket_summary(subs, cls, pay, file_total=50000)
    assert result["buckets"]["sub_on_file"]["count"]  == 1
    assert result["buckets"]["sub_on_file"]["dollars"] == 50000.0


def test_gap_ccs_vendor_goes_to_sub_with_gap():
    cls  = [_make_cls("Beta Inc", "ccs_vendor", "v2")]
    subs = [_make_sub("Beta Inc", "gap", 30000)]
    pay  = {"Beta Inc": _make_pay("Beta Inc", 30000)}

    result = compute_bucket_summary(subs, cls, pay, file_total=30000)
    assert result["buckets"]["sub_with_gap"]["count"]   == 1
    assert result["buckets"]["sub_with_gap"]["dollars"]  == 30000.0


def test_supplier_bucket_counts_not_sub_and_supplier():
    cls = [
        _make_cls("Home Depot", "not_sub"),
        _make_cls("Acme Supply", "supplier"),
    ]
    pay = {
        "Home Depot":  _make_pay("Home Depot",  5000),
        "Acme Supply": _make_pay("Acme Supply", 8000),
    }
    result = compute_bucket_summary([], cls, pay, file_total=13000)
    assert result["buckets"]["supplier"]["count"]   == 2
    assert result["buckets"]["supplier"]["dollars"] == 13000.0


def test_needs_decision_prominent_in_result():
    cls  = [_make_cls("Mystery LLC", "unclear")]
    pay  = {"Mystery LLC": _make_pay("Mystery LLC", 15000)}
    result = compute_bucket_summary([], cls, pay, file_total=15000)
    assert result["needs_decision_count"]   == 1
    assert result["needs_decision_dollars"] == 15000.0


def test_reconciliation_passes_when_sum_equals_total():
    cls  = [_make_cls("Sub A", "ccs_vendor", "v1"), _make_cls("Sub B", "not_sub")]
    subs = [_make_sub("Sub A", "full", 20000)]
    pay  = {
        "Sub A": _make_pay("Sub A", 20000),
        "Sub B": _make_pay("Sub B",  5000),
    }
    result = compute_bucket_summary(subs, cls, pay, file_total=25000)
    assert result["reconciled"] is True
    assert result["total_dollars"] == 25000.0


def test_reconciliation_fails_when_sum_differs():
    cls  = [_make_cls("Sub A", "ccs_vendor", "v1")]
    subs = [_make_sub("Sub A", "full", 20000)]
    pay  = {"Sub A": _make_pay("Sub A", 20000)}
    result = compute_bucket_summary(subs, cls, pay, file_total=19000)
    assert result["reconciled"] is False


def test_all_buckets_present_in_output():
    result = compute_bucket_summary([], [], {}, file_total=0)
    for key in BUCKET_LABELS:
        assert key in result["buckets"]


def test_empty_audit_reconciles_to_zero():
    result = compute_bucket_summary([], [], {}, file_total=0)
    assert result["reconciled"] is True
    assert result["total_dollars"] == 0.0
