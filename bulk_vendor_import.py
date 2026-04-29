"""
Bulk Vendor Import — Carolina Compliance Solutions
Reads a CSV file and imports vendors into Airtable for a specific client.

Usage:
    python bulk_vendor_import.py <client_name> <csv_file>

CSV format (with header row):
    vendor_name,contact_name,email,phone,trade
"""

import os
import sys
import csv
import logging
from pyairtable import Api
from dotenv import load_dotenv
import config

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_client_record(api, client_name):
    table = api.table(config.AIRTABLE_BASE_ID, "Clients")
    records = table.all(formula=f'{{Client Name}}="{client_name}"')
    if not records:
        logger.error("Client not found: %s", client_name)
        sys.exit(1)
    return records[0]


def import_vendors(client_name, csv_path):
    api = Api(config.AIRTABLE_API_KEY)
    vendors_table = api.table(config.AIRTABLE_BASE_ID, "Vendors")
    client_vendors_table = api.table(config.AIRTABLE_BASE_ID, "tblYPs2h9jxT3OL9H")
    assignments_table = api.table(config.AIRTABLE_BASE_ID, "tblpYKywfs0YHiQ98")

    client = get_client_record(api, client_name)
    client_id = client["id"]
    client_portal_email = (
        client["fields"].get("fldmh1sYahgN5x6KQ")  # Primary Contact Email
        or client["fields"].get("Primary Contact Email")
        or ""
    ).strip()
    if not client_portal_email:
        logger.warning("Client %s has no Primary Contact Email — Softr portal filtering will not work", client_name)
    logger.info("Found client: %s (ID: %s, Portal Email: %s)", client_name, client_id, client_portal_email or "MISSING")

    # Build set of existing vendor names for this client (via junction table)
    existing_junctions = client_vendors_table.all(
        formula=f"FIND('{client_id}', ARRAYJOIN({{Client}}))"
    )
    existing_vendor_ids = set()
    for j in existing_junctions:
        for vid in j["fields"].get("Vendors", []):
            existing_vendor_ids.add(vid)
    existing_vendor_names = set()
    for vid in existing_vendor_ids:
        try:
            v = vendors_table.get(vid)
            existing_vendor_names.add((v["fields"].get("Vendor Name", "")).strip().lower())
        except Exception:
            pass

    imported_names = []
    skipped_names = []
    failed_names = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vendor_name = row.get("vendor_name", "").strip()
            email = row.get("email", "").strip()

            if not vendor_name or not email:
                logger.warning("Skipping row — missing vendor name or email: %s", row)
                skipped_names.append(vendor_name or "(blank)")
                continue

            if vendor_name.strip().lower() in existing_vendor_names:
                logger.info("Duplicate skipped: %s", vendor_name)
                skipped_names.append(vendor_name)
                continue

            try:
                vendor_fields = {
                    "Vendor Name": vendor_name,
                    "Vendor Email": email,
                    "Compliance Status": "Needs Review",
                    "Send Request": True,
                }
                # Write the client's portal email to the dedicated Client Email field
                # so Softr can filter vendors per GC. Vendor Email (set above from CSV)
                # remains the actual vendor's address used for Initial Requests.
                if client_portal_email:
                    vendor_fields["fldyPBBSXqYnvjhL0"] = client_portal_email
                vendor_record = vendors_table.create(vendor_fields)
                new_vendor_id = vendor_record["id"]

                client_vendors_table.create({
                    "Client": [client_id],
                    "Vendors": [new_vendor_id],
                    "Tracking Status": "Active",
                })

                assignments_table.create({
                    "Vendor Link": [new_vendor_id],
                    "Client Link": [client_id],
                    "Active": True,
                    "Compliance Status": "Needs Review",
                })

                logger.info("Imported: %s (%s)", vendor_name, email)
                existing_vendor_names.add(vendor_name.strip().lower())
                imported_names.append(vendor_name)
            except Exception as e:
                logger.error("Error importing %s: %s", vendor_name, e)
                failed_names.append(vendor_name)

    logger.info(
        "=== Import Complete === Imported: %d | Skipped: %d | Errors: %d",
        len(imported_names), len(skipped_names), len(failed_names),
    )

    _send_import_summary(client_name, imported_names, skipped_names, failed_names)


def _send_import_summary(client_name, imported_names, skipped_names, failed_names):
    """Print summary to terminal and send an internal email to the owner."""
    total = len(imported_names) + len(skipped_names) + len(failed_names)

    imported_list = "\n".join(f"  - {n}" for n in imported_names) if imported_names else "  (none)"
    skipped_list = "\n".join(f"  - {n}" for n in skipped_names) if skipped_names else "  (none)"
    failed_list = "\n".join(f"  - {n}" for n in failed_names) if failed_names else "  (none)"

    body = (
        f"Vendor import for {client_name} is complete.\n\n"
        f"Total rows processed: {total}\n"
        f"Imported: {len(imported_names)}\n"
        f"Skipped (duplicate or missing data): {len(skipped_names)}\n"
        f"Failed: {len(failed_names)}\n\n"
        f"Imported vendors:\n{imported_list}\n\n"
    )
    if skipped_names:
        body += f"Skipped vendors:\n{skipped_list}\n\n"
    if failed_names:
        body += f"Failed vendors:\n{failed_list}\n\n"

    body += (
        f"REMINDER: Confirm that {client_name}'s Requirements Status is set to "
        f"'Received' in Airtable. Initial COI requests will not send until "
        f"requirements are confirmed.\n\n"
        f"Carolina Compliance Solutions"
    )

    subject = f"Vendor Import Complete — {client_name} — {len(imported_names)} vendors added"

    # Print to terminal
    print(f"\n{'=' * 60}")
    print(subject)
    print('=' * 60)
    print(body)
    print('=' * 60)

    # Send via SendGrid
    sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
    from_email = config.FROM_EMAIL
    to_email = config.OWNER_EMAIL

    if not sendgrid_api_key or not from_email:
        logger.warning("SendGrid not configured — import summary email not sent")
        return

    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, Email
        from legal_disclaimer import EMAIL_DISCLAIMER

        sg = SendGridAPIClient(api_key=sendgrid_api_key)
        message = Mail(
            from_email=Email(from_email, "Carolina Compliance Solutions"),
            to_emails=to_email,
            subject=subject,
            plain_text_content=body + EMAIL_DISCLAIMER,
        )
        message.reply_to = Email(config.INBOUND_EMAIL)
        sg.send(message)
        logger.info("Import summary email sent to %s", to_email)
    except Exception as e:
        logger.error("Failed to send import summary email: %s", e)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bulk_vendor_import.py <client_name> <csv_file>")
        sys.exit(1)
    import_vendors(sys.argv[1], sys.argv[2])
