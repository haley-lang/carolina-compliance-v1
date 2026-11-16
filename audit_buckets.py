"""audit_buckets.py — Payee bucket assignment and audit integrity checks.

No I/O — pure functions only. All data is passed in, nothing read from disk or DB.
"""
from __future__ import annotations

BUCKET_LABELS: dict[str, str] = {
    "sub_on_file":       "Sub — on file",
    "sub_with_gap":      "Sub — gap in coverage",
    "sub_no_ccs_record": "Sub — not in CCS",
    "supplier":          "Supplier / materials",
    "owner_payroll":     "Owner / payroll",
    "needs_decision":    "Needs decision",
}

BUCKET_ORDER = list(BUCKET_LABELS.keys())


def payee_bucket(payee_group: str, status: str = "not_in_ccs") -> str:
    """Map (payee_group, coverage_status) → bucket key.

    status values from _compute_audit_analysis:
        'full' | 'short_gap' | 'gap' | 'none_in_window' | 'no_policy' | 'not_in_ccs'
    """
    if payee_group == "ccs_vendor":
        return "sub_on_file" if status == "full" else "sub_with_gap"
    if payee_group == "likely_sub":
        return "sub_no_ccs_record"
    if payee_group in ("supplier", "not_sub"):
        return "supplier"
    if payee_group == "owner_payroll":
        return "owner_payroll"
    return "needs_decision"  # unclear or anything unrecognised


def compute_bucket_summary(
    analysis_subs: list[dict],
    all_cls: list[dict],
    pay_by_payee: dict[str, dict],
    file_total: float,
) -> dict:
    """Return per-bucket totals and a reconciliation summary.

    analysis_subs   — list from _compute_audit_analysis (has 'status', 'payee_raw')
    all_cls         — all payee_classifications rows for this audit
    pay_by_payee    — payee_raw → {total_amount, ...} from get_payee_summary
    file_total      — in-period total from audit_files

    Returns:
        {
          "buckets": {bucket_key: {"label", "count", "dollars"}},
          "total_dollars": float,
          "file_total": float,
          "reconciled": bool,
          "needs_decision_count": int,
          "needs_decision_dollars": float,
        }
    """
    buckets: dict[str, dict] = {
        k: {"label": v, "count": 0, "dollars": 0.0}
        for k, v in BUCKET_LABELS.items()
    }

    analysis_status = {s["payee_raw"]: s.get("status", "not_in_ccs") for s in analysis_subs}

    for cls in all_cls:
        pr    = cls["payee_raw"]
        pg    = cls.get("payee_group", "unclear")
        total = float((pay_by_payee.get(pr) or {}).get("total_amount") or 0)
        status = analysis_status.get(pr, "not_in_ccs")

        bucket = payee_bucket(pg, status)
        buckets[bucket]["count"]   += 1
        buckets[bucket]["dollars"] += total

    # Round dollars for display
    for b in buckets.values():
        b["dollars"] = round(b["dollars"], 2)

    total_dollars = round(sum(b["dollars"] for b in buckets.values()), 2)
    reconciled    = abs(total_dollars - file_total) < 1.0

    return {
        "buckets":                buckets,
        "total_dollars":          total_dollars,
        "file_total":             round(file_total, 2),
        "reconciled":             reconciled,
        "needs_decision_count":   buckets["needs_decision"]["count"],
        "needs_decision_dollars": buckets["needs_decision"]["dollars"],
    }


def compute_integrity_checks(
    bucket_summary: dict,
    all_cls: list[dict],
    pay_by_payee: dict[str, dict],
    analysis_subs: list[dict],
    file_rows_total: float,
) -> list[dict]:
    """Return 4 pass/fail integrity checks.

    Returns list of:
        {"check": str, "pass": bool, "detail": str}
    """
    checks = []

    # 1. Bucket sum reconciles with file total
    total_dollars = bucket_summary["total_dollars"]
    reconciled    = bucket_summary["reconciled"]
    checks.append({
        "check": "File total matches",
        "pass":  reconciled,
        "detail": (
            f"Bucket total ${total_dollars:,.2f} matches file ${file_rows_total:,.2f}"
            if reconciled else
            f"Bucket total ${total_dollars:,.2f} differs from file ${file_rows_total:,.2f} "
            f"(gap ${abs(total_dollars - file_rows_total):,.2f})"
        ),
    })

    # 2. All payees in payments have a classification
    payee_raws_in_file = set(pay_by_payee.keys())
    payee_raws_in_cls  = {c["payee_raw"] for c in all_cls}
    unclassified = payee_raws_in_file - payee_raws_in_cls
    checks.append({
        "check": "All payees classified",
        "pass":  not unclassified,
        "detail": (
            f"All {len(payee_raws_in_file)} payees have a classification"
            if not unclassified else
            f"{len(unclassified)} payee(s) in file have no classification: "
            + ", ".join(sorted(unclassified)[:3])
            + (" …" if len(unclassified) > 3 else "")
        ),
    })

    # 3. Payee counts match between file and classifications
    file_count = len(payee_raws_in_file)
    cls_count  = len(payee_raws_in_cls)
    counts_match = file_count == cls_count
    checks.append({
        "check": "Payee counts match",
        "pass":  counts_match,
        "detail": (
            f"{file_count} unique payees in file and classifications agree"
            if counts_match else
            f"File has {file_count} unique payees, classifications has {cls_count}"
        ),
    })

    # 4. No vendor's analysis total exceeds its payments-file total
    vendor_analysis: dict[str, float] = {}
    vendor_file: dict[str, float]     = {}

    for sub in analysis_subs:
        vid = sub.get("vendor_id", "")
        if not vid:
            continue
        vendor_analysis[vid] = vendor_analysis.get(vid, 0.0) + float(sub.get("total_paid") or 0)

    for cls in all_cls:
        vid = cls.get("vendor_id", "")
        if not vid:
            continue
        total = float((pay_by_payee.get(cls["payee_raw"]) or {}).get("total_amount") or 0)
        vendor_file[vid] = vendor_file.get(vid, 0.0) + total

    over_vendors = [
        vid for vid, ana_total in vendor_analysis.items()
        if ana_total > vendor_file.get(vid, 0.0) + 1.0
    ]
    checks.append({
        "check": "Vendor totals consistent",
        "pass":  not over_vendors,
        "detail": (
            "All vendor totals are consistent with the file"
            if not over_vendors else
            f"{len(over_vendors)} vendor(s) show analysis total exceeding file total"
        ),
    })

    return checks
