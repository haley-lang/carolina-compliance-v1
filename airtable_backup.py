"""
Airtable Backup — Carolina Compliance Solutions
Exports all core Airtable tables to CSV and sends a summary email.

Usage:
    python airtable_backup.py
"""

import csv
import logging
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BACKUP_DIR = Path(__file__).parent / "backups"

# Tables to export: (display name, Airtable table name or ID)
TABLES = [
    ("Clients", "Clients"),
    ("Vendors", "Vendors"),
    ("Insurance Policies", "Insurance Policies"),
    ("Insurance Certificates", "Insurance Certificates"),
    ("Incoming Extractions", "Incoming Extractions"),
    ("Client Requirements", "Client Requirements"),
    ("Vendor Client Assignments", "Vendor Client Assignments"),
    ("Email Queue", "Email Queue"),
    ("Compliance Log", "tblxdt7DT6V3JCQcW"),
]


def export_table_to_csv(api: Api, table_name: str, csv_path: Path) -> int:
    """Export all records from an Airtable table to a CSV file. Returns row count."""
    table = api.table(config.AIRTABLE_BASE_ID, table_name)
    records = table.all()

    if not records:
        # Write empty CSV with just a header placeholder
        csv_path.write_text("id\n")
        return 0

    # Collect all field names across all records
    all_fields = set()
    for r in records:
        all_fields.update(r.get("fields", {}).keys())
    field_names = ["id"] + sorted(all_fields)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=field_names, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            row = {"id": r["id"]}
            for k, v in r.get("fields", {}).items():
                # Flatten lists to comma-separated strings for CSV
                if isinstance(v, list):
                    row[k] = ", ".join(str(i) for i in v)
                else:
                    row[k] = v
            writer.writerow(row)

    return len(records)


def send_summary_email(today_str: str, results: list, failures: list):
    """Send backup summary to the owner via SendGrid."""
    sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
    if not sendgrid_api_key or not config.FROM_EMAIL:
        logger.warning("SendGrid not configured — backup summary email not sent")
        return

    subject = f"Weekly Airtable Backup Complete — {today_str}"

    lines = [f"Airtable backup completed on {today_str}.\n"]
    total_rows = 0
    for name, count in results:
        lines.append(f"  {name}: {count} rows")
        total_rows += count
    lines.append(f"\nTotal: {total_rows} rows across {len(results)} tables.")

    if failures:
        lines.append(f"\n{len(failures)} table(s) failed to export:")
        for name, error in failures:
            lines.append(f"  {name}: {error}")

    lines.append(f"\nBackup files saved to: backups/")

    body = "\n".join(lines)

    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, Email
        from legal_disclaimer import EMAIL_DISCLAIMER

        sg = SendGridAPIClient(api_key=sendgrid_api_key)
        message = Mail(
            from_email=Email(config.FROM_EMAIL, "Carolina Compliance Solutions"),
            to_emails=config.OWNER_EMAIL,
            subject=subject,
            plain_text_content=body + EMAIL_DISCLAIMER,
        )
        message.reply_to = Email(config.INBOUND_EMAIL)
        sg.send(message)
        logger.info("Backup summary email sent to %s", config.OWNER_EMAIL)
    except Exception as e:
        logger.error("Failed to send backup summary email: %s", e)


def run():
    logger.info("=== Airtable Backup starting ===")

    today_str = date.today().isoformat()
    BACKUP_DIR.mkdir(exist_ok=True)

    api = Api(config.AIRTABLE_API_KEY)

    results = []
    failures = []

    for display_name, table_id in TABLES:
        safe_name = display_name.replace(" ", "_")
        csv_path = BACKUP_DIR / f"{safe_name}_{today_str}.csv"

        try:
            count = export_table_to_csv(api, table_id, csv_path)
            results.append((display_name, count))
            logger.info("Exported %s: %d rows → %s", display_name, count, csv_path.name)
        except Exception as e:
            failures.append((display_name, str(e)))
            logger.error("Failed to export %s: %s", display_name, e)

    logger.info(
        "=== Backup complete === Exported: %d tables | Failed: %d",
        len(results), len(failures),
    )

    send_summary_email(today_str, results, failures)


if __name__ == "__main__":
    run()
