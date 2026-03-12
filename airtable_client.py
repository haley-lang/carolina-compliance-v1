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


def create_document_record(sender: str, subject: str, date_received: str, attachment_paths: list[str]) -> dict:
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
        "Status": "Pending Review",
    }

    record = get_table().create(fields)
    logger.info("Airtable record created — ID: %s", record["id"])
    return record
