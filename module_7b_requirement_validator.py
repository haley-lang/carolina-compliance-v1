import os
import logging
from pyairtable import Api
from datetime import date
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Airtable configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# Table names
TABLE_VENDORS = "Vendors"
TABLE_CLIENT_REQUIREMENTS = "Client Requirements"
TABLE_INSURANCE_POLICIES = "Insurance Policies"

# Compliance status values
STATUS_COMPLIANT = "Compliant"
STATUS_MISSING_COVERAGE = "Missing Coverage"
STATUS_EXPIRED = "Expired"
STATUS_NEEDS_REVIEW = "Needs Review"

def fetch_records(table):
    """Fetch all records from a given table."""
    try:
        records = table.all()
        logger.info("Loaded %d records from table", len(records))
        return records
    except Exception as e:
        logger.error("Failed to fetch records: %s", e)
        return []

def fetch_active_assignments_for_vendor(vendor_id, assignments_table):
    """Fetch active assignments for a specific vendor."""
    try:
        records = assignments_table.all()
        filtered_records = [
            record for record in records
            if record["fields"].get("Vendor Link", [None])[0] == vendor_id and record["fields"].get("Active") == True
        ]
        logger.info("Loaded %d active assignments for vendor %s", len(filtered_records), vendor_id)
        return filtered_records
    except Exception as e:
        logger.error("Failed to fetch active assignments: %s", e)
        return []

def fetch_policies_for_vendor(vendor_id, insurance_policies_table):
    """Fetch insurance policies for a specific vendor."""
    try:
        records = insurance_policies_table.all()
        filtered_records = [
            record for record in records
            if record["fields"].get("Vendor Link", [None])[0] == vendor_id
        ]
        logger.info("Loaded %d policies for vendor %s", len(filtered_records), vendor_id)
        return filtered_records
    except Exception as e:
        logger.error("Failed to fetch policies: %s", e)
        return []

def fetch_requirements_for_client(client_id, client_requirements_table):
    """Fetch client requirements for a specific client."""
    try:
        records = client_requirements_table.all()
        filtered_records = [
            record for record in records
            if record["fields"].get("Client Link", [None])[0] == client_id
        ]
        logger.info("Loaded %d requirements for client %s", len(filtered_records), client_id)
        return filtered_records
    except Exception as e:
        logger.error("Failed to fetch requirements: %s", e)
        return []

def validate_vendor(vendor, client_requirements, insurance_policies):
    """Validate a vendor's compliance status."""
    vendor_id = vendor["id"]
    vendor_name = vendor["fields"].get("Vendor Name", vendor_id)
    logger.info("Validating vendor: %s", vendor_name)

    # Find linked client requirements
    client_id = vendor["fields"].get("Client Link", [None])[0]
    if not client_id:
        logger.warning("Vendor %s has no linked client", vendor_name)
        return STATUS_NEEDS_REVIEW

    # Filter client requirements for this client
    client_reqs = [req for req in client_requirements if req["fields"].get("Client Link", [None])[0] == client_id]

    # Check if client requirements are empty
    if len(client_reqs) == 0:
        logger.warning("Vendor %s linked client has no requirements", vendor_name)
        return STATUS_NEEDS_REVIEW

    # Filter insurance policies for this vendor
    vendor_policies = [policy for policy in insurance_policies if policy["fields"].get("Vendor Link", [None])[0] == vendor_id]

    compliance_status = STATUS_COMPLIANT

    # Validate each requirement
    for req in client_reqs:
        policy_type = req["fields"].get("Policy Type")
        required = req["fields"].get("Required", False)

        # Check for required policy
        matching_policies = [p for p in vendor_policies if p["fields"].get("Policy Type") == policy_type]
        if required and not matching_policies:
            logger.info("Missing required policy: %s", policy_type)
            return STATUS_MISSING_COVERAGE

        # Check for expired policy
        for policy in matching_policies:
            expiry_date = policy["fields"].get("Expiration Date")
            if expiry_date and date.fromisoformat(expiry_date) < date.today():
                logger.info("Expired policy: %s", policy_type)
                return STATUS_EXPIRED

        # Check for endorsement requirements
        for field in ["Additional Insured", "Waiver", "Primary Noncontributory"]:
            if req["fields"].get(f"{field} Required", False) and not any(p["fields"].get(field) for p in matching_policies):
                logger.info("Missing endorsement: %s", field)
                return STATUS_NEEDS_REVIEW

    return STATUS_COMPLIANT

def update_vendor_status(vendors_table, vendor_id, status):
    """Update the compliance status of a vendor."""
    try:
        vendors_table.update(vendor_id, {"Compliance Status": status})
        logger.info("Updated vendor %s to status: %s", vendor_id, status)
    except Exception as e:
        logger.error("Failed to update vendor status: %s", e)

def evaluate_assignment(vendor, client_reqs, insurance_policies, assignment, assignments_table):
    """Evaluate vendor against a specific client assignment and update status."""
    vendor_id = vendor["id"]
    vendor_name = vendor["fields"].get("Vendor Name", vendor_id)
    logger.info("Evaluating assignment for vendor: %s", vendor_name)

    compliance_status = STATUS_COMPLIANT

    # Validate each requirement
    for req in client_reqs:
        policy_type = req["fields"].get("Policy Type")
        required = req["fields"].get("Required", False)

        # Check for required policy
        vendor_policies = [p for p in insurance_policies if p["fields"].get("Vendor Link", [None])[0] == vendor_id]
        matching_policies = [p for p in vendor_policies if p["fields"].get("Policy Type") == policy_type]
        if required and not matching_policies:
            logger.info("Missing required policy: %s", policy_type)
            compliance_status = STATUS_MISSING_COVERAGE
            break

        # Check for expired policy
        for policy in matching_policies:
            expiry_date = policy["fields"].get("Expiration Date")
            if expiry_date and date.fromisoformat(expiry_date) < date.today():
                logger.info("Expired policy: %s", policy_type)
                compliance_status = STATUS_EXPIRED
                compliance_status = STATUS_EXPIRED
                break
            if compliance_status == STATUS_EXPIRED:
                break

        if compliance_status != STATUS_EXPIRED:
            # Check for endorsement requirements
            for field in ["Additional Insured", "Waiver", "Primary Noncontributory"]:
                if req["fields"].get(f"{field} Required", False) and not any(p["fields"].get(field) for p in matching_policies):
                    logger.info("Missing endorsement: %s", field)
                    compliance_status = STATUS_NEEDS_REVIEW
                    break
            else:
                continue
            break
        if compliance_status == STATUS_EXPIRED:
            break
    else:
        compliance_status = STATUS_COMPLIANT

    # Update assignment status
    try:
        assignments_table.update(assignment["id"], {
            "Compliance Status": compliance_status,
            "Last Evaluated": date.today().isoformat()
        })
        logger.info("Updated assignment for vendor %s to status: %s", vendor_name, compliance_status)
    except Exception as e:
        logger.error("Failed to update assignment status: %s", e)

def run():
    logger.info("=== Module 7B: Requirement Validator starting ===")

    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        logger.error("Airtable API credentials are missing.")
        return

    api = Api(AIRTABLE_API_KEY)
    vendors_table = api.table(AIRTABLE_BASE_ID, TABLE_VENDORS)
    client_requirements_table = api.table(AIRTABLE_BASE_ID, TABLE_CLIENT_REQUIREMENTS)
    insurance_policies_table = api.table(AIRTABLE_BASE_ID, TABLE_INSURANCE_POLICIES)
    assignments_table = api.table(AIRTABLE_BASE_ID, "Vendor Client Assignments")
    vendors = fetch_records(vendors_table)

    for vendor in vendors:
        vendor_id = vendor["id"]
        vendor_name = vendor["fields"].get("Vendor Name", vendor_id)

        # Fetch active assignments for the vendor
        active_assignments = fetch_active_assignments_for_vendor(vendor_id, assignments_table)

        if active_assignments:
            for assignment in active_assignments:
                client_id = assignment["fields"].get("Client Link", [None])[0]
                if not client_id:
                    logger.warning("Assignment for vendor %s has no linked client", vendor_name)
                    continue

                # Fetch client requirements for this client
                client_reqs = fetch_requirements_for_client(client_id, client_requirements_table)

                # Fetch vendor policies
                vendor_policies = fetch_policies_for_vendor(vendor_id, insurance_policies_table)

                # Evaluate assignment
                evaluate_assignment(vendor, client_reqs, vendor_policies, assignment, assignments_table)
        else:
            # Use legacy path
            client_requirements = fetch_requirements_for_client(vendor["fields"].get("Client Link", [None])[0], client_requirements_table)
            insurance_policies = fetch_policies_for_vendor(vendor_id, insurance_policies_table)
            status = validate_vendor(vendor, client_requirements, insurance_policies)
            update_vendor_status(vendors_table, vendor_id, status)

    logger.info("=== Requirement validation complete ===")

if __name__ == "__main__":
    run()