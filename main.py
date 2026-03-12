"""
Carolina Compliance Solutions — COI Email Monitor
--------------------------------------------------
Connects to an IMAP inbox, downloads PDF/image COI attachments,
saves them to the uploads/ folder, and creates a record in Airtable.

Usage:
    python main.py
"""

import logging

from utils import setup_logging
import config
from email_monitor import connect_imap, fetch_unread_emails
from airtable_client import create_document_record

setup_logging()
logger = logging.getLogger(__name__)


def run():
    logger.info("=== Carolina Compliance Solutions — COI Monitor starting ===")

    config.validate_config()

    server = connect_imap()
    try:
        emails = fetch_unread_emails(server)
    finally:
        server.logout()

    if not emails:
        logger.info("No new COI documents found. Done.")
        return

    logger.info("Processing %d email(s) with qualifying attachments.", len(emails))

    for entry in emails:
        logger.info(
            "Creating Airtable record — Sender: %s | Subject: %s | Date: %s | Files: %s",
            entry["sender"],
            entry["subject"],
            entry["date_received"],
            entry["attachments"],
        )
        create_document_record(
            sender=entry["sender"],
            subject=entry["subject"],
            date_received=entry["date_received"],
            attachment_paths=entry["attachments"],
        )

    logger.info("=== Done. %d record(s) created in Airtable. ===", len(emails))


if __name__ == "__main__":
    run()
