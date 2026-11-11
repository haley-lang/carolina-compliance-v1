"""pulse_cron.py — System health check runner for Carolina Compliance Solutions.

Designed to be called by a Railway cron service every 15 minutes.

Usage:
    .venv/bin/python pulse_cron.py

Reads AIRTABLE_API_KEY and AIRTABLE_BASE_ID from environment (same .env as the
rest of the pipeline).  Writes results to health_store (SQLite-backed).
Prints a plain-text summary to stdout so Railway captures it in logs.
"""

import os
import sys
import time
import traceback
from datetime import datetime, timezone, date, timedelta

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# health_store is a sibling module in the same directory.
# ---------------------------------------------------------------------------
import health_store


# ---------------------------------------------------------------------------
# Airtable helpers
# ---------------------------------------------------------------------------

def _api():
    from pyairtable import Api
    key = (os.environ.get("AIRTABLE_API_KEY") or "").strip()
    if not key:
        raise EnvironmentError("AIRTABLE_API_KEY is not set")
    return Api(key)


def _base_id() -> str:
    raw = (os.environ.get("AIRTABLE_BASE_ID") or "").strip()
    # Strip the "app" prefix guard used elsewhere in the pipeline
    return raw.lstrip("/")


def _table(name: str):
    return _api().table(_base_id(), name)


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_airtable_space() -> dict:
    """Count records in every table and roll up totals."""
    result = {
        "total_records": None,
        "limit": 50_000,
        "level": "ok",
        "tables": [],
        "fastest_growing": None,
    }
    try:
        from pyairtable import Api
        api = _api()
        base_id = _base_id()

        # pyairtable doesn't have a metadata endpoint for counts, so we
        # list records for each known table with only one field (fastest).
        table_names = [
            "Incoming Documents",
            "Incoming Extractions",
            "Insurance Policies",
            "Insurance Certificates",
            "Vendors",
            "Clients",
            "Email Queue",
            "Compliance Log",
            "Vendor Client Assignments",
            "Client Requirements",
        ]

        rows = []
        total = 0
        for tname in table_names:
            try:
                tbl = api.table(base_id, tname)
                # Fetch only the record IDs (no fields) for speed
                records = tbl.all(fields=["Name"])  # any lightweight field
                count = len(records)
                total += count
                rows.append({"name": tname, "count": count, "growth_7d": 0})
            except Exception as exc:
                # Table might not exist or has different field; skip gracefully
                rows.append({"name": tname, "count": 0, "growth_7d": None})

        rows.sort(key=lambda r: r["count"], reverse=True)
        result["total_records"] = total
        result["tables"] = rows[:5]  # top 5 by count

        # Determine overall level
        pct = total / result["limit"]
        if pct >= 0.80:
            result["level"] = "error"
        elif pct >= 0.60:
            result["level"] = "warn"

        # Fastest growing = largest table (proxy; real growth needs history)
        if rows:
            result["fastest_growing"] = rows[0]["name"]

        health_store.record_event(
            category="airtable_space",
            level=result["level"],
            title=f"Airtable record count: {total:,} / {result['limit']:,}",
            body=f"{pct*100:.1f}% used",
            table_name=None,
        )
    except Exception as exc:
        result["level"] = "error"
        health_store.record_event(
            category="airtable_space",
            level="error",
            title="Failed to check Airtable record count",
            body=traceback.format_exc(limit=4),
            table_name=None,
        )
    return result


def check_email_intake() -> dict:
    """Count today's Incoming Documents by status."""
    result = {
        "last_check": None,
        "last_received": None,
        "today_received": 0,
        "today_processed": 0,
        "today_skipped": 0,
        "skipped_items": [],
        "level": "ok",
    }
    try:
        today_str = date.today().isoformat()  # e.g. "2026-11-04"
        tbl = _table("Incoming Documents")

        # Fetch documents from the last 48 hours (Airtable filter by date)
        all_docs = tbl.all(
            fields=["Subject", "From", "Date Received", "Status", "Skip Reason"],
            sort=["Date Received"],
        )

        last_received_ts = None
        skipped_items = []

        for rec in all_docs:
            f = rec["fields"]
            received_raw = (f.get("Date Received") or "").strip()
            if not received_raw:
                continue

            # Only count today's records
            if not received_raw.startswith(today_str):
                # Still track the latest timestamp for "last received"
                try:
                    ts = datetime.fromisoformat(received_raw.replace("Z", "+00:00"))
                    if last_received_ts is None or ts > last_received_ts:
                        last_received_ts = ts
                except ValueError:
                    pass
                continue

            result["today_received"] += 1

            status = (f.get("Status") or "").strip()
            if status in ("Skipped", "Not needed", "Not a COI"):
                result["today_skipped"] += 1
                skipped_items.append({
                    "id": rec["id"],
                    "subject": f.get("Subject") or "(no subject)",
                    "sender": f.get("From") or "",
                    "received_at": received_raw,
                    "reason": f.get("Skip Reason") or status,
                })
            elif status:
                result["today_processed"] += 1

            # Track last received
            try:
                ts = datetime.fromisoformat(received_raw.replace("Z", "+00:00"))
                if last_received_ts is None or ts > last_received_ts:
                    last_received_ts = ts
            except ValueError:
                pass

        if last_received_ts:
            result["last_received"] = last_received_ts.isoformat()

        result["skipped_items"] = skipped_items

        # Last check = heartbeat from pulse_cron itself
        last_hb = health_store.get_last_heartbeat("pulse_cron")
        if last_hb:
            result["last_check"] = datetime.fromtimestamp(last_hb, tz=timezone.utc).isoformat()

        # Level: warn if any skipped, error if many skipped
        if result["today_skipped"] > 5:
            result["level"] = "error"
        elif result["today_skipped"] > 0:
            result["level"] = "warn"

        health_store.record_event(
            category="email_intake",
            level=result["level"],
            title=f"Email intake today: {result['today_received']} received, "
                  f"{result['today_processed']} processed, {result['today_skipped']} skipped",
            body=None,
            table_name="Incoming Documents",
        )
    except Exception as exc:
        result["level"] = "error"
        health_store.record_event(
            category="email_intake",
            level="error",
            title="Failed to check email intake stats",
            body=traceback.format_exc(limit=4),
            table_name="Incoming Documents",
        )
    return result


def check_pipeline_stages() -> dict:
    """Count Incoming Extractions by Processing Status."""
    result = {
        "by_stage": {
            "received": 0,
            "extracted": 0,
            "pending_review": 0,
            "confirmed": 0,
        },
        "oldest_pending_days": None,
        "avg_days_received_to_confirmed": None,
        "stuck": [],
        "level": "ok",
    }
    try:
        tbl = _table("Incoming Extractions")
        all_recs = tbl.all(
            fields=["Processing Status", "Review Status", "Created", "Error Message"],
            sort=["Created"],
        )

        oldest_pending = None
        now = datetime.now(tz=timezone.utc)
        stuck = []

        for rec in all_recs:
            f = rec["fields"]
            status = (f.get("Processing Status") or "").strip().lower()
            review = (f.get("Review Status") or "").strip().lower()

            # Map to our stage buckets
            if status in ("received", "new", ""):
                result["by_stage"]["received"] += 1
            elif status in ("extracted", "imported"):
                result["by_stage"]["extracted"] += 1
            elif status in ("pending review", "needs review", "pending_review"):
                result["by_stage"]["pending_review"] += 1
                created_raw = (f.get("Created") or "").strip()
                if created_raw:
                    try:
                        created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
                        age_days = (now - created_dt).days
                        if oldest_pending is None or age_days > oldest_pending:
                            oldest_pending = age_days
                    except ValueError:
                        pass
            elif status in ("confirmed", "approved", "processed"):
                result["by_stage"]["confirmed"] += 1

            # Identify stuck records (error message present)
            err = (f.get("Error Message") or "").strip()
            if err:
                filename = rec.get("fields", {}).get("Source Filename") or rec["id"]
                stuck.append({
                    "filename": filename,
                    "stage": status,
                    "error": err[:120],
                })

        result["oldest_pending_days"] = oldest_pending
        result["stuck"] = stuck[:5]  # cap at 5

        # Determine level
        if oldest_pending is not None and oldest_pending > 7:
            result["level"] = "error"
        elif oldest_pending is not None and oldest_pending > 3:
            result["level"] = "warn"
        elif stuck:
            result["level"] = "warn"

        health_store.record_event(
            category="pipeline",
            level=result["level"],
            title="Pipeline stage counts: "
                  + ", ".join(f"{k}={v}" for k, v in result["by_stage"].items()),
            body=f"Stuck: {len(stuck)}, oldest pending: {oldest_pending}d",
            table_name="Incoming Extractions",
        )
    except Exception as exc:
        result["level"] = "error"
        health_store.record_event(
            category="pipeline",
            level="error",
            title="Failed to check pipeline stages",
            body=traceback.format_exc(limit=4),
            table_name="Incoming Extractions",
        )
    return result


def check_write_rate() -> dict:
    """Check recent Airtable write rate for the Compliance Log table."""
    result = {
        "writes_per_hour": [],
        "level": "ok",
    }
    try:
        tables_to_check = [
            ("Compliance Log", 50),
            ("Insurance Policies", 30),
            ("Incoming Extractions", 40),
        ]
        any_runaway = False
        for tname, normal_band in tables_to_check:
            count = health_store.get_write_rate(tname, window_minutes=60)
            over = count > normal_band
            if over:
                any_runaway = True
            result["writes_per_hour"].append({
                "table": tname,
                "count": count,
                "normal_band": normal_band,
            })

        if any_runaway:
            result["level"] = "warn"

        health_store.record_event(
            category="runaway",
            level=result["level"],
            title="Write rate check complete",
            body=", ".join(f"{r['table']}:{r['count']}" for r in result["writes_per_hour"]),
            table_name=None,
        )
    except Exception as exc:
        result["level"] = "error"
        health_store.record_event(
            category="runaway",
            level="error",
            title="Failed to check write rate",
            body=traceback.format_exc(limit=4),
            table_name=None,
        )
    return result


def check_outside_services() -> list:
    """Probe each external service and return a status list."""
    services = []

    # 1. Airtable API reachability
    try:
        tbl = _table("Incoming Documents")
        tbl.all(fields=["Date Received"], max_records=1)
        services.append({
            "name": "Airtable API",
            "status": "Reachable",
            "last_ok": datetime.now(tz=timezone.utc).isoformat(),
            "level": "ok",
            "detail": "",
        })
    except Exception as exc:
        services.append({
            "name": "Airtable API",
            "status": "Unreachable",
            "last_ok": None,
            "level": "error",
            "detail": str(exc)[:120],
        })

    # 2. Anthropic AI key validity (simple list-models call)
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=(os.environ.get("ANTHROPIC_API_KEY") or "").strip())
        # A minimal ping — list models endpoint or just validate key format
        api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        if not api_key or not api_key.startswith("sk-"):
            raise ValueError("ANTHROPIC_API_KEY missing or invalid format")
        # Attempt a minimal API call to validate (count_tokens is cheap)
        client.messages.count_tokens(
            model="claude-haiku-4-5",
            messages=[{"role": "user", "content": "ping"}],
        )
        services.append({
            "name": "AI API (Anthropic)",
            "status": "Key valid",
            "last_ok": datetime.now(tz=timezone.utc).isoformat(),
            "level": "ok",
            "detail": "",
        })
    except Exception as exc:
        services.append({
            "name": "AI API (Anthropic)",
            "status": "Error",
            "last_ok": None,
            "level": "error",
            "detail": str(exc)[:120],
        })

    # 3. Gmail inbox IMAP login
    try:
        import imaplib
        email_addr = (os.environ.get("EMAIL_ADDRESS") or "").strip()
        email_pass = (os.environ.get("EMAIL_PASSWORD") or "").replace(" ", "").strip()
        if not email_addr or not email_pass:
            raise ValueError("EMAIL_ADDRESS or EMAIL_PASSWORD not set")
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(email_addr, email_pass)
        mail.logout()
        services.append({
            "name": "Gmail inbox (IMAP)",
            "status": "Login OK",
            "last_ok": datetime.now(tz=timezone.utc).isoformat(),
            "level": "ok",
            "detail": "",
        })
    except Exception as exc:
        services.append({
            "name": "Gmail inbox (IMAP)",
            "status": "Login failed",
            "last_ok": None,
            "level": "error",
            "detail": str(exc)[:120],
        })

    # 4. SendGrid reachability (simple HTTPS GET to v3/stats)
    try:
        import urllib.request as _ur
        sg_key = (os.environ.get("SENDGRID_API_KEY") or "").strip()
        if not sg_key:
            raise ValueError("SENDGRID_API_KEY not set")
        req = _ur.Request(
            "https://api.sendgrid.com/v3/user/credits",
            headers={"Authorization": f"Bearer {sg_key}"},
        )
        with _ur.urlopen(req, timeout=8) as resp:
            resp.read()
        services.append({
            "name": "SendGrid",
            "status": "Reachable",
            "last_ok": datetime.now(tz=timezone.utc).isoformat(),
            "level": "ok",
            "detail": "",
        })
    except Exception as exc:
        msg = str(exc)
        # 403 / sandbox mode is a warning, not error
        level = "warn" if "403" in msg or "sandbox" in msg.lower() else "error"
        status = "Sandbox mode" if "sandbox" in msg.lower() else ("Unreachable" if "403" not in msg else "API error")
        services.append({
            "name": "SendGrid",
            "status": status,
            "last_ok": None,
            "level": level,
            "detail": msg[:120],
        })

    # Record a consolidated event
    any_error = any(s["level"] == "error" for s in services)
    any_warn  = any(s["level"] == "warn" for s in services)
    level = "error" if any_error else ("warn" if any_warn else "ok")
    health_store.record_event(
        category="services",
        level=level,
        title="Outside services check: " + ", ".join(
            f"{s['name']}={s['status']}" for s in services
        ),
        body=None,
        table_name=None,
    )

    return services


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start = time.time()
    print(f"[pulse_cron] Starting health check — {datetime.now(tz=timezone.utc).isoformat()}")

    results = {}
    summary_lines = []

    # 1. Airtable space
    print("[pulse_cron] Checking Airtable space…")
    results["airtable_space"] = check_airtable_space()
    s = results["airtable_space"]
    summary_lines.append(
        f"  Airtable space: {s['total_records']:,} records "
        f"({(s['total_records']/s['limit']*100):.1f}%) — {s['level']}"
        if s["total_records"] is not None
        else f"  Airtable space: ERROR"
    )

    # 2. Email intake
    print("[pulse_cron] Checking email intake…")
    results["email_intake"] = check_email_intake()
    ei = results["email_intake"]
    summary_lines.append(
        f"  Email intake: {ei['today_received']} received, "
        f"{ei['today_processed']} processed, {ei['today_skipped']} skipped — {ei['level']}"
    )

    # 3. Pipeline stages
    print("[pulse_cron] Checking pipeline stages…")
    results["pipeline"] = check_pipeline_stages()
    p = results["pipeline"]
    summary_lines.append(
        f"  Pipeline: {p['by_stage']} — {p['level']}"
    )

    # 4. Write rate
    print("[pulse_cron] Checking write rate…")
    results["write_rate"] = check_write_rate()
    wr = results["write_rate"]
    summary_lines.append(
        f"  Write rate: {wr['writes_per_hour']} — {wr['level']}"
    )

    # 5. Outside services
    print("[pulse_cron] Checking outside services…")
    results["services"] = check_outside_services()
    for svc in results["services"]:
        summary_lines.append(f"  {svc['name']}: {svc['status']} — {svc['level']}")

    # 6. Record heartbeat
    health_store.record_heartbeat("pulse_cron")

    # 7. Prune old data
    try:
        health_store.prune_old_data(keep_days=7)
        print("[pulse_cron] Pruned events older than 7 days.")
    except Exception as exc:
        print(f"[pulse_cron] WARNING: prune_old_data failed: {exc}")

    elapsed = time.time() - start
    print(f"\n[pulse_cron] Done in {elapsed:.1f}s. Summary:")
    for line in summary_lines:
        print(line)

    # Overall level
    all_levels = [
        results["airtable_space"]["level"],
        results["email_intake"]["level"],
        results["pipeline"]["level"],
        results["write_rate"]["level"],
    ] + [s["level"] for s in results["services"]]

    if "error" in all_levels:
        print("\n[pulse_cron] OVERALL: ERROR")
        sys.exit(1)
    elif "warn" in all_levels:
        print("\n[pulse_cron] OVERALL: WARN")
    else:
        print("\n[pulse_cron] OVERALL: OK")


if __name__ == "__main__":
    main()
