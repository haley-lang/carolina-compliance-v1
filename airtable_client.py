import logging
from pyairtable import Api

import config

logger = logging.getLogger(__name__)

_table = None


def get_table():
    """Return a cached Airtable Table instance."""
    global _table
    if _table is None:
        api = Api(config.AIRTABLE_API_KEY)
        _table = api.table(config.AIRTABLE_BASE_ID, config.AIRTABLE_TABLE_NAME)
    return _table


def create_document_record(
    sender: str,
    subject: str,
    date_received: str,
    attachment_paths: list[str],
    status: str = "Pending Review",
) -> dict:
    """
    Create a record in the Airtable 'Incoming Documents' table.

    Expected Airtable columns:
        Sender Email   (Single line text)
        Subject        (Single line text)
        Date Received  (Single line text — ISO 8601)
        File Names     (Long text — comma-separated list of saved filenames)
        Status         (Single select — defaults to 'Pending Review')
    """
    filenames = ", ".join(attachment_paths)

    fields = {
        "Sender Email": sender,
        "Subject": subject,
        "Date Received": date_received,
        "File Names": filenames,
        "Status": status,
    }

    record = get_table().create(fields)
    logger.info("Airtable record created — ID: %s", record["id"])
    return record


def update_document_pdf_r2_key(record_id: str, r2_keys: str) -> dict:
    """Patch an Incoming Documents record with the R2 key(s) for its
    attachment(s). Pass a comma-separated string if multiple attachments.

    Used by email_monitor.py as the second half of the two-step write
    pattern: create record → upload to R2 with record_id in key → patch
    record with the resulting key(s).

    Raises whatever pyairtable raises on update failure. Caller is
    responsible for catching + logging so intake doesn't unwind.
    """
    record = get_table().update(record_id, {"PDF R2 Key": r2_keys})
    logger.info("Patched record %s with PDF R2 Key (%d key(s))",
                record_id, len(r2_keys.split(",")))
    return record
