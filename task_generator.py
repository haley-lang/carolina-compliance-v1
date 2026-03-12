"""
Module 6 — Task Generator
Scans all Insurance Policies and creates follow-up tasks in the Tasks table
for policies that are expired or expiring within 30 days.

Rules:
  - Missing Expiration Date         → skip
  - Expiration Date before today    → Task: "Expired policy follow-up"   Priority: Urgent
  - Expiration Date within 30 days  → Task: "Expiring policy renewal reminder"  Priority: High
  - No duplicate open tasks for the same policy + task name
"""

import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

TABLE_POLICIES = "Insurance Policies"
TABLE_TASKS    = "Tasks"

EXPIRY_WARNING_DAYS = 30

TASK_EXPIRED        = "Expired policy follow-up"
TASK_EXPIRING_SOON  = "Expiring policy renewal reminder"

_DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_base_id(raw: str) -> str:
    """Extract just the appXXXXXXXXXX portion from AIRTABLE_BASE_ID."""
    match = re.search(r"(app[A-Za-z0-9]{10,})", raw)
    if not match:
        raise ValueError(
            f"AIRTABLE_BASE_ID does not look like a valid base ID: '{raw}'. "
            "It should start with 'app' (e.g. appCGgww0Pt7KE04u)."
        )
    return match.group(1)


def parse_date(raw: str) -> Optional[date]:
    """Try multiple date formats; return a date object or None if unparseable."""
    cleaned = (raw or "").strip()
    if not cleaned:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    logger.warning("Could not parse date: '%s'", cleaned)
    return None


def classify_policy(expiry: date, today: date) -> Optional[Tuple[str, str]]:
    """
    Return (task_name, priority) if the policy needs a task, else None.
    """
    if expiry < today:
        return TASK_EXPIRED, "Urgent"
    if expiry <= today + timedelta(days=EXPIRY_WARNING_DAYS):
        return TASK_EXPIRING_SOON, "High"
    return None


# ── Duplicate check ───────────────────────────────────────────────────────────

def open_task_exists(
    existing_tasks: List[dict],
    task_name: str,
    policy_record_id: str,
) -> bool:
    """
    Return True if an open task with the same name already links to this policy.
    Checks against the in-memory list to avoid extra API calls.
    """
    for task in existing_tasks:
        fields = task["fields"]
        if fields.get("Task Name") != task_name:
            continue
        if fields.get("Status") != "Open":
            continue
        linked_policies = fields.get("Insurance Policy") or []
        if policy_record_id in linked_policies:
            return True
    return False


# ── Task creation ─────────────────────────────────────────────────────────────

def build_task_fields(
    task_name: str,
    priority: str,
    policy_record_id: str,
    policy_number: str,
    policy_type: str,
    expiry: date,
    vendor_link: List[str],
) -> dict:
    """Assemble the fields dict for a new Tasks record."""
    fields = {
        "Task Name":         task_name,
        "Status":            "Open",
        "Priority":          priority,
        "Insurance Policy":  [policy_record_id],
        "Notes": (
            f"Policy #: {policy_number} | "
            f"Type: {policy_type} | "
            f"Expiration: {expiry.strftime('%m/%d/%Y')}"
        ),
    }
    if vendor_link:
        fields["Vendor"] = vendor_link
    return fields


# ── Core logic ────────────────────────────────────────────────────────────────

def run():
    logger.info("=== Module 6: Task Generator starting ===")

    # ── Validate env ──────────────────────────────────────────────────────────
    api_key  = (config.AIRTABLE_API_KEY or "").strip()
    raw_base = (config.AIRTABLE_BASE_ID or "").strip()

    if not api_key or not raw_base:
        raise EnvironmentError(
            "AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set in .env"
        )

    base_id = clean_base_id(raw_base)
    logger.info("Airtable base ID : %s", base_id)

    api             = Api(api_key)
    policies_table  = api.table(base_id, TABLE_POLICIES)
    tasks_table     = api.table(base_id, TABLE_TASKS)

    today = date.today()
    logger.info("Evaluation date  : %s", today)

    # ── Load all records in two batch calls ───────────────────────────────────
    logger.info("Loading all Insurance Policies...")
    all_policies = policies_table.all()
    logger.info("Loaded %d policy record(s)", len(all_policies))

    logger.info("Loading all existing Tasks...")
    existing_tasks = tasks_table.all()
    logger.info("Loaded %d existing task(s)", len(existing_tasks))

    # ── Scan policies and generate tasks ──────────────────────────────────────
    created_count  = 0
    skipped_count  = 0
    no_date_count  = 0

    for policy in all_policies:
        record_id     = policy["id"]
        fields        = policy["fields"]
        policy_number = (fields.get("Policy Number")  or "").strip()
        policy_type   = (fields.get("Policy Type")    or "").strip()
        raw_expiry    = (fields.get("Expiration Date") or "").strip()

        label = policy_number or record_id

        # Rule 1: skip missing expiration date
        if not raw_expiry:
            logger.info("Policy %s — no Expiration Date, skipping", label)
            no_date_count += 1
            continue

        expiry = parse_date(raw_expiry)
        if expiry is None:
            logger.warning("Policy %s — unparseable date '%s', skipping", label, raw_expiry)
            no_date_count += 1
            continue

        # Classify
        result = classify_policy(expiry, today)
        if result is None:
            logger.info("Policy %s — expires %s, no action needed", label, expiry)
            continue

        task_name, priority = result
        logger.info(
            "Policy %s — expires %s → task '%s' [%s]",
            label, expiry, task_name, priority,
        )

        # Rule 4: duplicate check
        if open_task_exists(existing_tasks, task_name, record_id):
            logger.info("  Open task already exists — skipping duplicate")
            skipped_count += 1
            continue

        # Vendor link: Airtable may surface linked vendor IDs on the policy record
        vendor_link = fields.get("Vendor Link") or []
        if isinstance(vendor_link, str):
            vendor_link = [vendor_link]

        task_fields = build_task_fields(
            task_name      = task_name,
            priority       = priority,
            policy_record_id = record_id,
            policy_number  = policy_number,
            policy_type    = policy_type,
            expiry         = expiry,
            vendor_link    = vendor_link,
        )

        new_task = tasks_table.create(task_fields)
        logger.info("  Created task — ID: %s", new_task["id"])

        # Add to in-memory list so subsequent iterations can detect duplicates
        # within this same run without an extra API call
        existing_tasks.append(new_task)
        created_count += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("=== Task Generator complete ===")
    logger.info(
        "Summary — Tasks created: %d | Duplicates skipped: %d | No date (skipped): %d",
        created_count, skipped_count, no_date_count,
    )


if __name__ == "__main__":
    try:
        run()
    except (EnvironmentError, ValueError, RuntimeError) as exc:
        logger.error("Task generator failed: %s", exc)
        raise SystemExit(1) from exc
