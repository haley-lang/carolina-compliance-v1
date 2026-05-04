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

# Subscription-tier vendor caps. Mirrors module_16 webhook tiers.
# Pro is intentionally not present — V1 ships 3 tiers only.
TIER_CAPS = {"Starter": 30, "Growth": 100, "Scale": 300}


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

    # Build set of vendor IDs already assigned to this client.
    # We walk Vendor Client Assignments (the canonical V1 junction) and filter
    # in Python because Airtable's ARRAYJOIN on linked-record fields returns
    # primary-field text (vendor/client names), not record IDs — which is why
    # the previous FIND(client_id, ARRAYJOIN({Client})) approach silently
    # never matched, allowing duplicate vendor records on re-import.
    existing_assigned_vendor_ids = set()
    try:
        for a in assignments_table.all():
            if client_id in (a["fields"].get("Client Link") or []):
                for vid in (a["fields"].get("Vendor Link") or []):
                    existing_assigned_vendor_ids.add(vid)
    except Exception as e:
        logger.error("Failed to load assignments for dedup: %s", e)
        sys.exit(1)

    # ── Subscription-tier cap enforcement (hard-fail) ─────────────────────
    # Read the client's tier and refuse the whole import if existing+CSV
    # would exceed it. Hard-fail (not partial) — partial imports leave the
    # operator guessing which rows landed and which didn't.
    tier = (
        client["fields"].get("fldXnhsv3ntup5Gpy")  # Subscription Tier (field ID)
        or client["fields"].get("Subscription Tier")
    )
    if not tier:
        logger.error(
            "Client %s has no Subscription Tier — refusing import. "
            "Set tier on the Clients table (Starter / Growth / Scale) and retry.",
            client_name,
        )
        sys.exit(1)
    if tier not in TIER_CAPS:
        logger.error(
            "Client %s has unknown Subscription Tier '%s' — refusing import. "
            "Allowed tiers: %s",
            client_name, tier, ", ".join(sorted(TIER_CAPS.keys())),
        )
        sys.exit(1)
    cap = TIER_CAPS[tier]
    existing_count = len(existing_assigned_vendor_ids)
    try:
        with open(csv_path, newline="", encoding="utf-8") as _f:
            csv_count = sum(1 for _ in csv.DictReader(_f))
    except Exception as e:
        logger.error("Failed to read CSV for cap pre-check: %s", e)
        sys.exit(1)
    projected = existing_count + csv_count
    if projected > cap:
        logger.error(
            "Tier cap exceeded for %s — tier=%s cap=%d existing=%d csv_rows=%d projected=%d. "
            "Refusing import. Upgrade the client's Subscription Tier or trim the CSV.",
            client_name, tier, cap, existing_count, csv_count, projected,
        )
        sys.exit(1)
    logger.info(
        "Tier check OK for %s — tier=%s cap=%d existing=%d csv_rows=%d projected=%d",
        client_name, tier, cap, existing_count, csv_count, projected,
    )

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

            # Look up vendors by name (case-insensitive). If any match is
            # already assigned to this client, skip; otherwise create new.
            try:
                safe_name = vendor_name.strip().lower().replace("'", "\\'")
                name_matches = vendors_table.all(
                    formula=f"LOWER({{Vendor Name}}) = '{safe_name}'"
                )
            except Exception as e:
                logger.error("Failed to look up vendor %s for dedup: %s", vendor_name, e)
                failed_names.append(vendor_name)
                continue

            if any(v["id"] in existing_assigned_vendor_ids for v in name_matches):
                logger.info("Vendor %s already assigned to %s — skipping", vendor_name, client_name)
                skipped_names.append(vendor_name)
                continue

            try:
                vendor_fields = {
                    "Vendor Name": vendor_name,
                    "Vendor Email": email,
                    "Compliance Status": "Needs Review",
                    "Send Request": True,
                }
                # Client Email (fldyPBBSXqYnvjhL0) is a computed lookup field
                # on Vendors that auto-populates from the linked Client via
                # Vendor Client Assignment. Don't write it directly — Airtable
                # rejects with 422.
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
                existing_assigned_vendor_ids.add(new_vendor_id)
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
        message.reply_to = Email(config.reply_to_for("internal"))
        sg.send(message)
        logger.info("Import summary email sent to %s", to_email)
    except Exception as e:
        logger.error("Failed to send import summary email: %s", e)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bulk_vendor_import.py <client_name> <csv_file>")
        sys.exit(1)
    import_vendors(sys.argv[1], sys.argv[2])
