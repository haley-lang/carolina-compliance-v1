"""
Module 7A — Client Setup Wizard
Interactive terminal wizard to onboard a new client into Airtable.
Creates one Clients record and one Client Requirements record per selected policy type.
"""

import os
import json
import argparse
import logging
from dotenv import load_dotenv
from pathlib import Path
from pyairtable import Api
from pyairtable.formulas import match

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

CLIENTS_TABLE      = "Clients"
REQUIREMENTS_TABLE = "Client Requirements"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_args():
    parser = argparse.ArgumentParser(description="Client Setup Wizard")
    parser.add_argument("--config", type=str, required=True, help="Path to the JSON config file")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the process without making changes")
    return parser.parse_args()

def load_config(config_path):
    with open(config_path, 'r') as file:
        return json.load(file)

def validate_config(config):
    required_fields = ["name", "requirements"]
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing required field: {field}")
    for req in config["requirements"]:
        if "Policy Type" not in req or not req["Policy Type"]:
            raise ValueError("Each requirement must have a valid Policy Type.")
        if "Required" not in req:
            raise ValueError("Each requirement must specify if it is Required.")
        if "Additional Insured Required" not in req:
            raise ValueError("Each requirement must specify if Additional Insured is Required.")
        if "Waiver Required" not in req:
            raise ValueError("Each requirement must specify if Waiver is Required.")
        if "Primary Noncontributory Required" not in req:
            raise ValueError("Each requirement must specify if Primary Noncontributory is Required.")

def check_duplicate_client(clients_table, client_name):
    formula = match({"Client Name": client_name})
    existing_clients = clients_table.all(formula=formula)
    return len(existing_clients) > 0

def create_client_record(clients_table, client_info, dry_run):
    fields = {
        "Client Name": client_info["name"]
    }
    if dry_run:
        logging.info(f"DRY RUN: Would create client record with fields: {fields}")
        return "dry_run_id"
    try:
        record = clients_table.create(fields)
        logging.info(f"Clients record created — ID: {record['id']}")
        return record["id"]
    except Exception as e:
        logging.error(f"Failed to create client record: {e}")
        return None

def create_requirement_records(requirements_table, client_record_id, requirements, dry_run):
    created = 0
    for req in requirements:
        fields = {
            "Client Link": [client_record_id],
            "Policy Type": req["Policy Type"],
            "Required": req["Required"],
            "Additional Insured Required": req["Additional Insured Required"],
            "Waiver Required": req["Waiver Required"],
            "Primary Noncontributory Required": req["Primary Noncontributory Required"]
        }
        if "Minimum Limit" in req and req["Minimum Limit"]:
            fields["Minimum Limit"] = str(req["Minimum Limit"])
        if dry_run:
            logging.info(f"DRY RUN: Would create requirement record with fields: {fields}")
        else:
            try:
                record = requirements_table.create(fields)
                logging.info(f"Requirement created — {req['Policy Type']} — ID: {record['id']}")
            except Exception as e:
                logging.error(f"Failed to create requirement record for {req['Policy Type']}: {e}")
                continue
        created += 1

    return created

"""
Default Airtable fields written by this script:

Clients
- Client Name

Client Requirements
- Client Link
- Policy Type
- Required
- Additional Insured Required
- Waiver Required
- Primary Noncontributory Required
- Minimum Limit (default 0 if not specified)
"""
def run():
    args = parse_args()
    config = load_config(args.config)
    validate_config(config)

    api = Api(AIRTABLE_API_KEY)
    clients_table = api.table(AIRTABLE_BASE_ID, CLIENTS_TABLE)
    requirements_table = api.table(AIRTABLE_BASE_ID, REQUIREMENTS_TABLE)

    if check_duplicate_client(clients_table, config["name"]):
        logging.warning(f"Duplicate client detected: {config['name']}")
        return

    client_record_id = create_client_record(clients_table, config, args.dry_run)
    if client_record_id:
        create_requirement_records(requirements_table, client_record_id, config["requirements"], args.dry_run)

if __name__ == "__main__":
    run()
