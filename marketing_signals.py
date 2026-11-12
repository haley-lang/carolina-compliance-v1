"""
Marketing signals reader.
Reads existing Airtable activity and surfaces anonymized events
that could become content stories.

Sources:
  - Corrections (field changes + Haley's notes)
  - Gaps (repeated gap types across certificates)
  - Audits (completed audit work)
  - Caught expirations (policies about to lapse that were caught)
  - Milestone moments (first time vendor has every cert on file)

Each signal is returned as a dict ready to write to Marketing Signals.
Caller (marketing_story_spotter.py) writes them and calls Claude.
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

# IDs from review_app.py
CORRECTIONS_TABLE_ID  = "tblh6NTQikjzl8FFu"
GAP_REQUESTS_TABLE_ID = "tbleY5UzcLYZFZFKF"
AUDITS_TABLE_ID       = "tblbaTMKuM6yWeVjL"
VENDORS_TABLE_ID      = "tblsOphSd5DKSZEro"
INSURANCE_POLICIES_TABLE_ID = "tblpPcmm5ANE0bMNB"

LOOKBACK_DAYS = 14  # how far back to scan for signals


def _api() -> pyairtable.Api:
    return pyairtable.Api(os.environ["AIRTABLE_API_KEY"])


def _tbl(api: pyairtable.Api, table_id: str):
    return api.table(MARKETING_BASE_ID, table_id)


def _since_ts() -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    return cutoff.strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ── Signal gatherers ──────────────────────────────────────────────────────────

def gather_correction_signals(api: pyairtable.Api) -> list[dict]:
    """
    Scan recent Corrections rows.
    Every correction where Haley left a note is a potential story
    (shows the kind of mistake the system catches and why it matters).
    """
    signals = []
    tbl = _api().table(MARKETING_BASE_ID, CORRECTIONS_TABLE_ID)
    formula = f"IS_AFTER({{Created At}}, '{_since_ts()}')"
    try:
        records = tbl.all(formula=formula, fields=["Why (Haley's note)", "Field", "Old Value", "New Value", "Created At"])
    except Exception as exc:
        log.warning("correction signals fetch failed: %s", exc)
        return signals

    for rec in records:
        f = rec.get("fields", {})
        note = (f.get("Why (Haley's note)") or "").strip()
        field = (f.get("Field") or "").strip()
        if not note and not field:
            continue
        raw = f"Field: {field}. Note: {note}"
        summary = anonymize(raw)
        signals.append({
            MS_SOURCE: "correction",
            MS_SUMMARY: summary,
            MS_RAW_REF_ID: rec["id"],
            MS_CREATED_AT: f.get("Created At") or datetime.now(timezone.utc).isoformat(),
        })
    return signals


def gather_gap_signals(api: pyairtable.Api) -> list[dict]:
    """
    Scan Gap Requests for repeated gap types.
    When the same gap (e.g. missing WC) shows up 3+ times in a window,
    that pattern is a teachable moment for a GC audience.
    """
    signals = []
    tbl = _api().table(MARKETING_BASE_ID, GAP_REQUESTS_TABLE_ID)
    formula = f"IS_AFTER({{Created At}}, '{_since_ts()}')"
    try:
        records = tbl.all(formula=formula, fields=["Gap Type", "Policy Type", "Client", "Created At"])
    except Exception as exc:
        log.warning("gap signals fetch failed: %s", exc)
        return signals

    gap_counts: Counter = Counter()
    for rec in records:
        f = rec.get("fields", {})
        gap_type = (f.get("Gap Type") or f.get("Policy Type") or "").strip()
        if gap_type:
            gap_counts[gap_type] += 1

    for gap_type, count in gap_counts.items():
        if count >= 3:
            summary = anonymize(
                f"Gap type '{gap_type}' appeared {count} times across recent certificates."
            )
            signals.append({
                MS_SOURCE: "gap",
                MS_SUMMARY: summary,
                MS_RAW_REF_ID: f"gap:{gap_type}",
                MS_CREATED_AT: datetime.now(timezone.utc).isoformat(),
            })
    return signals


def gather_audit_signals(api: pyairtable.Api) -> list[dict]:
    """
    Scan completed audits.
    A completed audit is a milestone: the GC had everything ready.
    """
    signals = []
    tbl = _api().table(MARKETING_BASE_ID, AUDITS_TABLE_ID)
    formula = f"AND(IS_AFTER({{Created At}}, '{_since_ts()}'), {{Status}}='Complete')"
    try:
        records = tbl.all(formula=formula, fields=["Client", "Status", "Notes", "Created At"])
    except Exception as exc:
        log.warning("audit signals fetch failed: %s", exc)
        return signals

    for rec in records:
        f = rec.get("fields", {})
        notes = anonymize((f.get("Notes") or "").strip())
        summary = f"Audit completed successfully. {notes}".strip()
        signals.append({
            MS_SOURCE: "audit",
            MS_SUMMARY: summary,
            MS_RAW_REF_ID: rec["id"],
            MS_CREATED_AT: f.get("Created At") or datetime.now(timezone.utc).isoformat(),
        })
    return signals


def gather_expiration_signals(api: pyairtable.Api) -> list[dict]:
    """
    Scan recently caught expirations (policies with Expiration Status set
    to a warning threshold in the last LOOKBACK_DAYS window).
    Shows the system's proactive value.
    """
    signals = []
    tbl = _api().table(MARKETING_BASE_ID, INSURANCE_POLICIES_TABLE_ID)
    formula = (
        f"AND("
        f"IS_AFTER(LAST_MODIFIED_TIME(), '{_since_ts()}'),"
        f"OR({{Expiration Status}}='30 Days', {{Expiration Status}}='7 Days')"
        f")"
    )
    try:
        records = tbl.all(formula=formula, fields=["Policy Type", "Expiration Date", "Expiration Status"])
    except Exception as exc:
        log.warning("expiration signals fetch failed: %s", exc)
        return signals

    for rec in records:
        f = rec.get("fields", {})
        pol_type = (f.get("Policy Type") or "policy").strip()
        exp_status = (f.get("Expiration Status") or "").strip()
        exp_date = (f.get("Expiration Date") or "").strip()
        summary = anonymize(
            f"Caught expiring {pol_type} ({exp_status} notice, expires {exp_date}). "
            "Vendor notified with time to renew."
        )
        signals.append({
            MS_SOURCE: "milestone",
            MS_SUMMARY: summary,
            MS_RAW_REF_ID: rec["id"],
            MS_CREATED_AT: datetime.now(timezone.utc).isoformat(),
        })
    return signals


def gather_milestone_signals(api: pyairtable.Api) -> list[dict]:
    """
    Scan Vendors for 'first time fully on file' moments:
    vendors whose compliance status recently flipped to fully on file.
    """
    signals = []
    tbl = _api().table(MARKETING_BASE_ID, VENDORS_TABLE_ID)
    formula = (
        f"AND("
        f"IS_AFTER(LAST_MODIFIED_TIME(), '{_since_ts()}'),"
        f"{{Compliance Status}}='On File'"
        f")"
    )
    try:
        records = tbl.all(formula=formula, fields=["Compliance Status", "Vendor Name"])
    except Exception as exc:
        log.warning("milestone signals fetch failed: %s", exc)
        return signals

    for rec in records:
        summary = anonymize("A contractor got every required certificate on file. First time fully on file.")
        signals.append({
            MS_SOURCE: "milestone",
            MS_SUMMARY: summary,
            MS_RAW_REF_ID: rec["id"],
            MS_CREATED_AT: datetime.now(timezone.utc).isoformat(),
        })
    return signals


# ── Dedup against already-stored signals ─────────────────────────────────────

def _existing_ref_ids(api: pyairtable.Api) -> set[str]:
    """Return all Raw Reference IDs already in Marketing Signals table."""
    tbl = _api().table(MARKETING_BASE_ID, MARKETING_SIGNALS_TABLE_ID)
    try:
        records = tbl.all(fields=[MS_RAW_REF_ID])
        return {r["fields"].get(MS_RAW_REF_ID, "") for r in records if r.get("fields")}
    except Exception as exc:
        log.warning("Could not fetch existing signal IDs: %s", exc)
        return set()


# ── Public entry point ────────────────────────────────────────────────────────

def collect_new_signals() -> list[dict]:
    """
    Gather all signals from all sources, dedup against existing records,
    and return only the new ones (ready to write to Marketing Signals).
    """
    api = _api()
    new_signals: list[dict] = []
    for gatherer in (
        gather_correction_signals,
        gather_gap_signals,
        gather_audit_signals,
        gather_expiration_signals,
        gather_milestone_signals,
    ):
        try:
            new_signals.extend(gatherer(api))
        except Exception as exc:
            log.warning("gatherer %s failed: %s", gatherer.__name__, exc)

    existing = _existing_ref_ids(api)
    fresh = [s for s in new_signals if s.get(MS_RAW_REF_ID) not in existing]
    log.info("Collected %d new signals (%d total, %d already stored)",
             len(fresh), len(new_signals), len(existing))
    return fresh


def write_signals(signals: list[dict]) -> list[str]:
    """Write a list of signal dicts to the Marketing Signals table. Returns created record IDs."""
    if not signals:
        return []
    api = _api()
    tbl = api.table(MARKETING_BASE_ID, MARKETING_SIGNALS_TABLE_ID)
    created_ids = []
    for sig in signals:
        try:
            rec = tbl.create(sig)
            created_ids.append(rec["id"])
        except Exception as exc:
            log.warning("Failed to write signal: %s", exc)
    return created_ids
