import os
import email
import logging
from pathlib import Path
from imapclient import IMAPClient
from email.header import decode_header

import config
from utils import safe_filename, parse_email_date

logger = logging.getLogger(__name__)


def decode_mime_words(value: str) -> str:
    """Decode encoded email header values (e.g. UTF-8 subjects)."""
    parts = decode_header(value)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(part)
    return "".join(decoded)


def connect_imap() -> IMAPClient:
    """Connect and authenticate to the IMAP server."""
    logger.info("Connecting to %s:%s as %s", config.IMAP_HOST, config.IMAP_PORT, config.EMAIL_ADDRESS)
    server = IMAPClient(config.IMAP_HOST, port=config.IMAP_PORT, ssl=True)
    server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
    logger.info("Authenticated successfully.")
    return server


def fetch_unread_emails(server: IMAPClient) -> list[dict]:
    """
    Fetch unread emails from INBOX that have PDF or image attachments.

    Returns a list of dicts with keys:
        sender, subject, date_received, attachments (list of saved file paths)
    """
    server.select_folder("INBOX")
    message_ids = server.search(["UNSEEN"])

    if not message_ids:
        logger.info("No unread emails found.")
        return []

    logger.info("Found %d unread email(s).", len(message_ids))
    upload_dir = Path(config.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for msg_id in message_ids:
        raw = server.fetch([msg_id], ["RFC822"])
        msg = email.message_from_bytes(raw[msg_id][b"RFC822"])

        sender = decode_mime_words(msg.get("From", ""))
        subject = decode_mime_words(msg.get("Subject", "(no subject)"))
        date_str = msg.get("Date", "")
        date_received = parse_email_date(date_str)

        logger.info("Processing email — From: %s | Subject: %s | Date: %s", sender, subject, date_received)

        saved_files = []

        for part in msg.walk():
            content_disposition = part.get_content_disposition() or ""
            if "attachment" not in content_disposition:
                continue

            filename = part.get_filename()
            if not filename:
                continue

            filename = decode_mime_words(filename)
            ext = Path(filename).suffix.lower()

            if ext not in config.ALLOWED_EXTENSIONS:
                logger.debug("Skipping non-COI attachment: %s", filename)
                continue

            safe_name = safe_filename(filename)
            dest = upload_dir / safe_name

            # Avoid overwriting — append a counter if needed
            counter = 1
            while dest.exists():
                dest = upload_dir / f"{dest.stem}_{counter}{ext}"
                counter += 1

            dest.write_bytes(part.get_payload(decode=True))
            logger.info("Saved attachment: %s", dest)
            saved_files.append(str(dest))

        if saved_files:
            results.append(
                {
                    "sender": sender,
                    "subject": subject,
                    "date_received": date_received,
                    "attachments": saved_files,
                }
            )
        else:
            logger.info("No qualifying attachments in this email — skipping Airtable record.")

    return results
