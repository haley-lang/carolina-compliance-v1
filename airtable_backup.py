"""
Airtable Backup — Carolina Compliance Solutions
Exports all core Airtable tables to CSV, uploads each to Cloudflare R2,
and sends a summary email.

Usage:
    python airtable_backup.py

Cloudflare R2:
    Uploads each CSV to s3://{R2_BUCKET_NAME}/airtable-backups/{YYYY-MM-DD}/{TableName}.csv
    using the R2 S3-compatible endpoint at
    https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com.
    If any of R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID,
    R2_BUCKET_NAME is unset, R2 upload is skipped (local-only fallback).
    Per-table upload failures are logged and do not abort the run.
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
    ("Outbound Follow-Ups", "Outbound Follow-Ups"),
    ("Tasks", "Tasks"),
]


# ── Cloudflare R2 upload ─────────────────────────────────────────────────────

def _r2_client():
    """Construct an S3-compatible boto3 client for Cloudflare R2.

    Returns a tuple (client, bucket_name) on success or (None, None) if any
    required env var is missing — caller treats None as "skip R2 upload,
    fall back to local-only behavior."
    """
    access_key = os.getenv("R2_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    account_id = os.getenv("R2_ACCOUNT_ID")
    bucket = os.getenv("R2_BUCKET_NAME")

    missing = [
        name for name, val in [
            ("R2_ACCESS_KEY_ID", access_key),
            ("R2_SECRET_ACCESS_KEY", secret_key),
            ("R2_ACCOUNT_ID", account_id),
            ("R2_BUCKET_NAME", bucket),
        ] if not val
    ]
    if missing:
        logger.warning(
            "R2 upload skipped — missing env var(s): %s. "
            "Backup CSVs will be local-only.",
            ", ".join(missing),
        )
        return None, None

    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except ImportError as exc:
        logger.error("R2 upload skipped — boto3 not installed: %s", exc)
        return None, None

    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(signature_version="s3v4"),
        region_name="auto",
    )
    return client, bucket


def upload_csv_to_r2(client, bucket, csv_path: Path, key: str) -> bool:
    """Upload one CSV to R2. Returns True on success, False on failure.
    Failure is logged but never raises — caller continues with the next table.
    """
    try:
        client.upload_file(
            Filename=str(csv_path),
            Bucket=bucket,
            Key=key,
            ExtraArgs={"ContentType": "text/csv"},
        )
        return True
    except Exception as exc:
        logger.error("R2 upload failed for %s -> %s: %s", csv_path.name, key, exc)
        return False


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


def send_summary_email(today_str: str, results: list, failures: list,
                       r2_upload_report: dict = None):
    """Send backup summary to the owner via SendGrid.

    r2_upload_report (optional dict) shape:
        {
            "uploaded": [(table_name, key_path, bytes), ...],
            "failed":   [(table_name, key_path, error), ...],
            "skipped":  bool,                 # True if R2 was not configured
            "key_prefix": "airtable-backups/{date}/",
        }
    """
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

    # ── R2 upload report ──────────────────────────────────────────────────
    if r2_upload_report:
        if r2_upload_report.get("skipped"):
            lines.append(
                "\nCloudflare R2 upload SKIPPED — R2 env vars not configured."
            )
        else:
            uploaded = r2_upload_report.get("uploaded") or []
            failed = r2_upload_report.get("failed") or []
            total_bytes = sum(b for _, _, b in uploaded)
            key_prefix = r2_upload_report.get("key_prefix") or ""

            lines.append("\nCloudflare R2 upload:")
            lines.append(f"  Path prefix: s3://<R2_BUCKET_NAME>/{key_prefix}")
            lines.append(f"  Uploaded: {len(uploaded)} table(s), {total_bytes:,} bytes total")
            for tname, key, nbytes in uploaded:
                lines.append(f"    ✓ {tname}: {nbytes:,} bytes ({key})")
            if failed:
                lines.append(f"  Failed: {len(failed)} table(s)")
                for tname, key, error in failed:
                    lines.append(f"    ✗ {tname} ({key}): {error}")

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

    # Initialize R2 once. None values mean "skip upload" — local write proceeds.
    r2_client, r2_bucket = _r2_client()
    r2_skipped = r2_client is None
    r2_uploaded = []  # list of (display_name, key, bytes)
    r2_failed = []    # list of (display_name, key, error)
    r2_key_prefix = f"airtable-backups/{today_str}/"

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
            continue  # No CSV produced — skip R2 upload for this table

        # ── R2 upload (after local write, never aborts the run) ────────
        if r2_client is not None:
            r2_key = f"{r2_key_prefix}{display_name}.csv"
            ok = upload_csv_to_r2(r2_client, r2_bucket, csv_path, r2_key)
            if ok:
                try:
                    nbytes = csv_path.stat().st_size
                except Exception:
                    nbytes = 0
                r2_uploaded.append((display_name, r2_key, nbytes))
                logger.info("R2 uploaded: %s -> %s (%d bytes)", display_name, r2_key, nbytes)
            else:
                r2_failed.append((display_name, r2_key, "see prior log line"))

    logger.info(
        "=== Backup complete === Exported: %d tables | Failed: %d | "
        "R2 uploaded: %d | R2 failed: %d | R2 skipped: %s",
        len(results), len(failures), len(r2_uploaded), len(r2_failed), r2_skipped,
    )

    r2_report = {
        "uploaded": r2_uploaded,
        "failed": r2_failed,
        "skipped": r2_skipped,
        "key_prefix": r2_key_prefix,
    }
    send_summary_email(today_str, results, failures, r2_upload_report=r2_report)


if __name__ == "__main__":
    run()
