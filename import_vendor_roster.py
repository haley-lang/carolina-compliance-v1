#!/usr/bin/env python3
"""Import a vendor roster CSV into Airtable for a given client.

Usage:
    .venv/bin/python import_vendor_roster.py "path/to/roster.csv" "Client Name"
"""

import csv
import os
import re
import sys
from difflib import get_close_matches

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv(override=True)

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# --- Airtable table IDs ---
VENDORS_TABLE_ID = "tblsOphSd5DKSZEro"
CLIENTS_TABLE_ID = "tbltnBIWke20IEI3K"
VENDOR_CLIENT_ASSIGNMENT_TABLE_ID = "tblpYKywfs0YHiQ98"

# --- Vendors table field IDs ---
FLD_VENDOR_NAME = "fldbb0BUb3wggDMJMp"
FLD_VENDOR_EMAIL = "fldxteHtQ5ITcx6Zw"
FLD_VENDOR_CLIENT_LINK = "fld4x4QmMotnVVKoQ"
FLD_VENDOR_SOURCE = "fldiIeKME1rzERHhF"
FLD_VENDOR_CREATED_BY = "fld0VUtYgQ0Ayhpij"

# --- Vendor Client Assignment field IDs ---
FLD_VCA_VENDOR_LINK = "fld7ZjWTE652bGJve"
FLD_VCA_CLIENT_LINK = "fldT229klsR59yvqz"
FLD_VCA_ACTIVE = "fld7aAB0tYKJLzrEw"

# --- Fuzzy header matching candidates ---
COMPANY_CANDIDATES = ["company", "vendor", "sub", "sub name", "vendor name", "company name", "subcontractor"]
EMAIL_CANDIDATES = ["email", "e-mail", "email address", "vendor email", "contact email"]
PHONE_CANDIDATES = ["phone", "phone number", "telephone", "tel", "contact phone"]

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def fuzzy_match_header(header: str, candidates: list[str]) -> bool:
    """Return True if *header* fuzzy-matches any of the *candidates*."""
    normalized = header.strip().lower()
    if normalized in candidates:
        return True
    # Try substring matching
    for c in candidates:
        if c in normalized or normalized in c:
            return True
    # Try difflib fuzzy matching
    matches = get_close_matches(normalized, candidates, n=1, cutoff=0.6)
    return len(matches) > 0


def resolve_columns(headers: list[str]) -> dict:
    """Map CSV headers to our logical column names using fuzzy matching.

    Returns a dict like {"company": 0, "email": 2, "phone": 3} (index positions).
    Raises ValueError if required columns are missing.
    """
    mapping: dict[str, int | None] = {"company": None, "email": None, "phone": None}
    for idx, header in enumerate(headers):
        if mapping["company"] is None and fuzzy_match_header(header, COMPANY_CANDIDATES):
            mapping["company"] = idx
        elif mapping["email"] is None and fuzzy_match_header(header, EMAIL_CANDIDATES):
            mapping["email"] = idx
        elif mapping["phone"] is None and fuzzy_match_header(header, PHONE_CANDIDATES):
            mapping["phone"] = idx

    missing = []
    if mapping["company"] is None:
        missing.append("Company/Vendor/Sub Name")
    if mapping["email"] is None:
        missing.append("Email")
    if missing:
        raise ValueError(f"Required CSV columns not found: {', '.join(missing)}")

    return mapping


def validate_email(email: str) -> bool:
    """Return True if *email* looks like a valid email address."""
    return bool(EMAIL_REGEX.match(email.strip())) if email else False


def read_csv(csv_path: str) -> tuple[list[str], list[list[str]]]:
    """Read a CSV file and return (headers, rows)."""
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)
    return headers, rows


def lookup_client(api: Api, client_name: str) -> str | None:
    """Find a client record ID by name. Returns the record ID or None."""
    clients_table = api.table(AIRTABLE_BASE_ID, CLIENTS_TABLE_ID)
    formula = f"FIND(LOWER(\"{client_name.lower()}\"), LOWER({{Client Name}}))"
    records = clients_table.all(formula=formula)
    if records:
        return records[0]["id"]
    return None


def get_existing_vendors_for_client(api: Api, client_record_id: str) -> set[str]:
    """Return a set of lowercased vendor names already linked to this client."""
    vendors_table = api.table(AIRTABLE_BASE_ID, VENDORS_TABLE_ID)
    formula = f"FIND(\"{client_record_id}\", ARRAYJOIN({{Client Link}}))"
    try:
        records = vendors_table.all(formula=formula)
    except Exception:
        return set()

    names = set()
    for rec in records:
        fields = rec.get("fields", {})
        name = fields.get("Vendor Name", "")
        if isinstance(name, str) and name.strip():
            names.add(name.strip().lower())
    return names


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: .venv/bin/python import_vendor_roster.py \"path/to/roster.csv\" \"Client Name\"")
        sys.exit(1)

    csv_path = sys.argv[1]
    client_name = sys.argv[2]

    if not os.path.isfile(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)

    if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
        print("Error: Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID environment variables.")
        sys.exit(1)

    # --- Read and parse CSV ---
    headers, rows = read_csv(csv_path)
    col_map = resolve_columns(headers)

    print(f"\nParsing CSV: {csv_path}")
    print(f"Client: {client_name}")
    print(f"Total rows: {len(rows)}")
    print(f"Mapped columns: company={headers[col_map['company']]}, "
          f"email={headers[col_map['email']]}"
          f"{', phone=' + headers[col_map['phone']] if col_map.get('phone') is not None else ''}")
    print()

    # --- Connect to Airtable and look up the client ---
    api = Api(AIRTABLE_API_KEY)
    client_record_id = lookup_client(api, client_name)
    if not client_record_id:
        print(f"Error: Client '{client_name}' not found in Airtable Clients table.")
        sys.exit(1)
    print(f"Found client record: {client_record_id}")

    # --- Get existing vendors for this client ---
    existing_vendor_names = get_existing_vendors_for_client(api, client_record_id)

    # --- Validate rows ---
    vendors_ready = []
    invalid_email_list = []
    duplicate_csv_list = []
    already_in_airtable_list = []

    seen_names: dict[str, int] = {}  # lowercase name -> first row index

    for row_idx, row in enumerate(rows, start=2):  # start=2 because row 1 is headers
        company_idx = col_map["company"]
        email_idx = col_map["email"]
        phone_idx = col_map.get("phone")

        company = row[company_idx].strip() if company_idx < len(row) else ""
        email = row[email_idx].strip() if email_idx < len(row) else ""
        phone = row[phone_idx].strip() if phone_idx is not None and phone_idx < len(row) else ""

        if not company:
            continue  # skip blank rows

        # Validation 1: Valid email
        if not validate_email(email):
            invalid_email_list.append(company)
            continue

        # Validation 2: Duplicate company name within CSV
        company_lower = company.lower()
        if company_lower in seen_names:
            duplicate_csv_list.append(company)
            continue
        seen_names[company_lower] = row_idx

        # Validation 3: Already in Airtable for this client
        if company_lower in existing_vendor_names:
            already_in_airtable_list.append(company)
            continue

        vendors_ready.append({
            "company": company,
            "email": email,
            "phone": phone,
        })

    # --- Print summary report ---
    print("\n" + "=" * 50)
    print("IMPORT SUMMARY")
    print("=" * 50)
    print(f"{len(vendors_ready)} vendors ready to import")

    if invalid_email_list:
        print(f"{len(invalid_email_list)} skipped — invalid/missing email: {invalid_email_list}")
    else:
        print("0 skipped — invalid/missing email")

    if duplicate_csv_list:
        print(f"{len(duplicate_csv_list)} skipped — duplicate in CSV: {duplicate_csv_list}")
    else:
        print("0 skipped — duplicate in CSV")

    if already_in_airtable_list:
        print(f"{len(already_in_airtable_list)} skipped — already in Airtable: {already_in_airtable_list}")
    else:
        print("0 skipped — already in Airtable")

    print("=" * 50)

    if not vendors_ready:
        print("\nNo vendors to import. Exiting.")
        return

    # --- Ask for confirmation ---
    answer = input("\nProceed with import? (y/n): ").strip().lower()
    if answer != "y":
        print("Import cancelled.")
        return

    # --- Import vendors ---
    vendors_table = api.table(AIRTABLE_BASE_ID, VENDORS_TABLE_ID)
    vca_table = api.table(AIRTABLE_BASE_ID, VENDOR_CLIENT_ASSIGNMENT_TABLE_ID)

    imported_count = 0
    for vendor in vendors_ready:
        try:
            # Create Vendors record
            vendor_record = vendors_table.create({
                FLD_VENDOR_NAME: vendor["company"],
                FLD_VENDOR_EMAIL: vendor["email"],
                FLD_VENDOR_CLIENT_LINK: [client_record_id],
                FLD_VENDOR_SOURCE: "CSV Import",
                FLD_VENDOR_CREATED_BY: "Haley",
            })
            vendor_record_id = vendor_record["id"]

            # Create Vendor Client Assignment record
            vca_table.create({
                FLD_VCA_VENDOR_LINK: [vendor_record_id],
                FLD_VCA_CLIENT_LINK: [client_record_id],
                FLD_VCA_ACTIVE: True,
            })

            imported_count += 1
        except Exception as exc:
            print(f"Error importing {vendor['company']}: {exc}")

    print(f"\nImport complete. {imported_count} vendors added for {client_name}.")
    print("Run the pipeline to send initial COI requests.")


if __name__ == "__main__":
    main()
