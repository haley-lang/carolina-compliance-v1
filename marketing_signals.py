"""
Marketing signals reader.
Reads existing Airtable activity and surfaces anonymized events
that could become content stories.

Sources:
  - Corrections (field changes + Haley's notes) — Corrections table
  - Gaps (repeated coverage-type gaps across requests) — Gap Requests table
  - Audits (recently created audit workspaces) — Audits table
  - Caught expirations (Expiration Status = 'Expiring Soon') — Insurance Policies
  - Milestone moments (Compliance Status flipped to 'Matches Requirements') — Vendors
"""
from __future__ import annotations
import os
import logging
from datetime import datetime, timezone, timedelta
from collections import Counter

import pyairtable

from marketing_constants import (
    MARKETING_BASE_ID,
    MARKETING_SIGNALS_TABLE_ID, MS_SUMMARY, MS_SOURCE, MS_RAW_REF_ID, MS_CREATED_AT,
)
from anonymizer import anonymize

log = logging.getLogger(__name__)

BASE_ID = MARKETING_BASE_ID  # all tables live in the same base

# Table IDs — real IDs from review_app.py
CORRECTIONS_TABLE_ID        = "tblh6NTQikjzl8FFu"
GAP_REQUESTS_TABLE_ID       = "tbleY5UzcLYZFZFKF"
AUDITS_TABLE_ID             = "tblbaTMKuM6yWeVjL"
INSURANCE_POLICIES_TABLE_ID = "tblpPcmm5ANE0bMNB"
VENDORS_TABLE_ID            = "tblsOphSd5DKSZEro"

LOOKBACK_DAYS = 14


def _api() -> pyairtable.Api:
    return pyairtable.Api(os.environ["AIRTABLE_API_KEY"])


def _tbl(table_id: str):
    return _api().table(BASE_ID, table_id)


def _since_ts() -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    return cutoff.strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ── Signal gatherers ──────────────────────────────────────────────────────────

def gather_correction_signals() -> list[dict]:
    """
    Recent Corrections rows with a reviewer note.
    Every correction where Haley left a note is a potential story
    (shows the kind of mistake the system catches and why it matters).
    """
    signals = []
    formula = f"AND(IS_AFTER({{Corrected At}}, '{_since_ts()}'), {{Why (Haley's note)}}!='')"
    try:
        records = _tbl(CORRECTIONS_TABLE_ID).all(
            formula=formula,
            fields=["Why (Haley's note)", "Field", "Old Value", "New Value", "Corrected At"],
        )
    except Exception as exc:
        log.warning("correction signals fetch failed: %s", exc)
        return signals

    for rec in records:
        f = rec.get("fields", {})
        note  = (f.get("Why (Haley's note)") or "").strip()
        field = (f.get("Field") or "").strip()
        if not note and not field:
            continue
        raw = f"Field corrected: {field}. Reviewer note: {note}"
        signals.append({
            MS_SOURCE: "correction",
            MS_SUMMARY: anonymize(raw),
            MS_RAW_REF_ID: rec["id"],
            MS_CREATED_AT: f.get("Corrected At") or datetime.now(timezone.utc).isoformat(),
        })
    return signals


def gather_gap_signals() -> list[dict]:
    """
    Count gap requests by Coverage Type over the lookback window.
    When the same coverage type shows up 3+ times it's a teachable pattern.
    """
    signals = []
    formula = f"IS_AFTER({{Created At}}, '{_since_ts()}')"
    try:
        records = _tbl(GAP_REQUESTS_TABLE_ID).all(
            formula=formula,
            fields=["Coverage Type", "Created At"],
        )
    except Exception as exc:
        log.warning("gap signals fetch failed: %s", exc)
        return signals

    gap_counts: Counter = Counter()
    for rec in records:
        cov = (rec.get("fields", {}).get("Coverage Type") or "").strip()
        if cov:
            gap_counts[cov] += 1

    for cov_type, count in gap_counts.items():
        if count >= 3:
            summary = anonymize(
                f"Coverage gap type '{cov_type}' appeared {count} times "
                f"across recent certificate requests in the last {LOOKBACK_DAYS} days."
            )
            signals.append({
                MS_SOURCE: "gap",
                MS_SUMMARY: summary,
                MS_RAW_REF_ID: f"gap:{cov_type}",
                MS_CREATED_AT: datetime.now(timezone.utc).isoformat(),
            })
    return signals


def gather_audit_signals() -> list[dict]:
    """
    Recent audit workspace creations. Each audit created = a GC was preparing.
    """
    signals = []
    formula = f"IS_AFTER({{Created At}}, '{_since_ts()}')"
    try:
        records = _tbl(AUDITS_TABLE_ID).all(
            formula=formula,
            fields=["Audit Name", "Audit Type", "Created At"],
        )
    except Exception as exc:
        log.warning("audit signals fetch failed: %s", exc)
        return signals

    for rec in records:
        f = rec.get("fields", {})
        audit_type = (f.get("Audit Type") or "").strip()
        summary = anonymize(
            f"Audit workspace started for {audit_type} coverage audit. "
            "Certificates pulled and reviewed for the audit window."
        )
        signals.append({
            MS_SOURCE: "audit",
            MS_SUMMARY: summary,
            MS_RAW_REF_ID: rec["id"],
            MS_CREATED_AT: f.get("Created At") or datetime.now(timezone.utc).isoformat(),
        })
    return signals


def gather_expiration_signals() -> list[dict]:
    """
    Policies recently flagged as Expiring Soon — the system caught it proactively.
    """
    signals = []
    formula = (
        f"AND("
        f"IS_AFTER(LAST_MODIFIED_TIME(), '{_since_ts()}'),"
        f"{{Expiration Status}}='Expiring Soon'"
        f")"
    )
    try:
        records = _tbl(INSURANCE_POLICIES_TABLE_ID).all(
            formula=formula,
            fields=["Policy Type", "Expiration Date", "Expiration Status"],
        )
    except Exception as exc:
        log.warning("expiration signals fetch failed: %s", exc)
        return signals

    for rec in records:
        f = rec.get("fields", {})
        pol_type = (f.get("Policy Type") or "policy").strip()
        exp_date = (f.get("Expiration Date") or "").strip()
        summary = anonymize(
            f"Caught expiring {pol_type} certificate (expires {exp_date}). "
            "Vendor notified with enough lead time to renew."
        )
        signals.append({
            MS_SOURCE: "milestone",
            MS_SUMMARY: summary,
            MS_RAW_REF_ID: rec["id"],
            MS_CREATED_AT: datetime.now(timezone.utc).isoformat(),
        })
    return signals


def gather_milestone_signals() -> list[dict]:
    """
    Vendors recently reaching 'Matches Requirements' — fully on file milestone.
    """
    signals = []
    formula = (
        f"AND("
        f"IS_AFTER(LAST_MODIFIED_TIME(), '{_since_ts()}'),"
        f"{{Compliance Status}}='Matches Requirements'"
        f")"
    )
    try:
        records = _tbl(VENDORS_TABLE_ID).all(
            formula=formula,
            fields=["Compliance Status", "Vendor Name"],
        )
    except Exception as exc:
        log.warning("milestone signals fetch failed: %s", exc)
        return signals

    for rec in records:
        summary = anonymize(
            "A contractor got every required certificate on file and meets requirements. "
            "First time the system shows them fully on file."
        )
        signals.append({
            MS_SOURCE: "milestone",
            MS_SUMMARY: summary,
            MS_RAW_REF_ID: rec["id"],
            MS_CREATED_AT: datetime.now(timezone.utc).isoformat(),
        })
    return signals


# ── Dedup ─────────────────────────────────────────────────────────────────────

def _existing_ref_ids() -> set[str]:
    """Return all Raw Reference IDs already in Marketing Signals."""
    try:
        records = _tbl(MARKETING_SIGNALS_TABLE_ID).all(fields=[MS_RAW_REF_ID])
        return {r["fields"].get(MS_RAW_REF_ID, "") for r in records if r.get("fields")}
    except Exception as exc:
        log.warning("Could not fetch existing signal IDs: %s", exc)
        return set()


# ── Public entry point ────────────────────────────────────────────────────────

def collect_new_signals() -> list[dict]:
    """
    Gather all signals, dedup against existing records, return only new ones.
    """
    all_signals: list[dict] = []
    for gatherer in (
        gather_correction_signals,
        gather_gap_signals,
        gather_audit_signals,
        gather_expiration_signals,
        gather_milestone_signals,
    ):
        try:
            all_signals.extend(gatherer())
        except Exception as exc:
            log.warning("gatherer %s raised: %s", gatherer.__name__, exc)

    existing = _existing_ref_ids()
    fresh = [s for s in all_signals if s.get(MS_RAW_REF_ID) not in existing]
    log.info(
        "Signals: %d new of %d gathered (%d already stored)",
        len(fresh), len(all_signals), len(all_signals) - len(fresh),
    )
    return fresh


def write_signals(signals: list[dict]) -> list[str]:
    """Write signal dicts to Marketing Signals. Returns created record IDs."""
    if not signals:
        return []
    tbl = _tbl(MARKETING_SIGNALS_TABLE_ID)
    ids = []
    for sig in signals:
        try:
            rec = tbl.create(sig)
            ids.append(rec["id"])
        except Exception as exc:
            log.warning("Failed to write signal %s: %s", sig.get(MS_RAW_REF_ID), exc)
    return ids
