"""
Bulk Vendor Import — Carolina Compliance Solutions
Reads a CSV file and imports vendors into Airtable for a specific client.

Usage:
    python bulk_vendor_import.py <client_name> <csv_file>

CSV format (with header row):
    vendor_name,contact_name,email,phone,trade
"""

import sys
import csv
import logging
from pyairtable import Api
import config

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
    logger.info("Found client: %s (ID: %s)", client_name, client_id)

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

    imported = 0
    skipped = 0
    errors = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vendor_name = row.get("vendor_name", "").strip()
            email = row.get("email", "").strip()

            if not vendor_name or not email:
                logger.warning("Skipping row — missing vendor name or email: %s", row)
                skipped += 1
                continue

            if vendor_name.strip().lower() in existing_vendor_names:
                logger.info("Duplicate skipped: %s", vendor_name)
                skipped += 1
                continue

            try:
                vendor_record = vendors_table.create({
                    "Vendor Name": vendor_name,
                    "Vendor Email": email,
                    "Compliance Status": "Pending",
                    "Send Request": True,
                })
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
                    "Compliance Status": "Pending Review",
                })

                logger.info("Imported: %s (%s)", vendor_name, email)
                existing_vendor_names.add(vendor_name.strip().lower())
                imported += 1
            except Exception as e:
                logger.error("Error importing %s: %s", vendor_name, e)
                errors += 1

    logger.info("=== Import Complete === Imported: %d | Skipped: %d | Errors: %d", imported, skipped, errors)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bulk_vendor_import.py <client_name> <csv_file>")
        sys.exit(1)
    import_vendors(sys.argv[1], sys.argv[2])
