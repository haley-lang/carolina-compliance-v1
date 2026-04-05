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

    client = get_client_record(api, client_name)
    client_id = client["id"]
    logger.info("Found client: %s (ID: %s)", client_name, client_id)

    imported = 0
    skipped = 0
    errors = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vendor_name = row.get("vendor_name", "").strip()
            contact_name = row.get("contact_name", "").strip()
            email = row.get("email", "").strip()
            phone = row.get("phone", "").strip()
            trade = row.get("trade", "").strip()

            if not vendor_name or not email:
                logger.warning("Skipping row — missing vendor name or email: %s", row)
                skipped += 1
                continue

            try:
                fields = {
                    "Vendor Name": vendor_name,
                    "Vendor Email": email,
                    "Client Vendors": [client_id],
                    "Compliance Status": "Pending",
                }
                vendors_table.create(fields)
                logger.info("Imported: %s (%s)", vendor_name, email)
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
