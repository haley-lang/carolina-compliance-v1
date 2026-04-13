import os
import email
import logging
import traceback
from pathlib import Path
from imapclient import IMAPClient
from email.header import decode_header
from dotenv import load_dotenv

load_dotenv()

import config
from utils import safe_filename, parse_email_date
from airtable_client import create_document_record, get_table
from email_classifier import (
    classify_email, write_classification_to_airtable, SKIP_EVENTS,
    EVENT_BOUNCE, EVENT_AUTO_REPLY, EVENT_CLOUD_LINK, _extract_body_text,
)
from operational_email_handler import (
    handle_bounce, handle_cloud_link, gate_oversize_attachment,
    gate_unsupported_type, check_attachment_size,
)
print("EMAIL MONITOR STARTED")
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
    try:
        logger.info("Connecting to %s:%s as %s", config.IMAP_HOST, config.IMAP_PORT, config.EMAIL_ADDRESS)
        print("[email_monitor] Login starting...")
        server = IMAPClient(config.IMAP_HOST, port=config.IMAP_PORT, ssl=True)
        server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
        logger.info("Authenticated successfully.")
        return server
    except Exception as e:
        print(f"[email_monitor] Exception during IMAP login: {e}")
        raise


def fetch_unread_emails(server: IMAPClient) -> list[dict]:
    """
    Fetch unread emails from INBOX that have PDF or image attachments.

    Returns a list of dicts with keys:
        sender, subject, date_received, attachments (list of saved file paths)
    """
    try:
        server.select_folder("INBOX")
        print("[email_monitor] INBOX connection succeeded: True")
    except Exception as e:
        print("[email_monitor] INBOX connection succeeded: False")
        print(f"[email_monitor] Exception selecting INBOX: {e}")
        raise

    try:
        message_ids = server.search(["UNSEEN"])
    except Exception as e:
        print(f"[email_monitor] Exception searching unread emails: {e}")
        raise

    print(f"[email_monitor] Unread emails found: {len(message_ids)}")

    if not message_ids:
        logger.info("No unread emails found.")
        return []

    logger.info("Found %d unread email(s).", len(message_ids))
    upload_dir = Path(config.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for msg_id in message_ids:
        try:
            raw = server.fetch([msg_id], ["RFC822"])
            msg = email.message_from_bytes(raw[msg_id][b"RFC822"])

            sender = decode_mime_words(msg.get("From", ""))
            subject = decode_mime_words(msg.get("Subject", "(no subject)"))
            date_str = msg.get("Date", "")
            date_received = parse_email_date(date_str)

            logger.info("Processing email — From: %s | Subject: %s | Date: %s", sender, subject, date_received)
            print(f"[email_monitor] Checking email subject: {subject}")

            saved_files = []
            all_attachment_filenames = []
            found_attachment = False
            staging_status = "Pending Review"
            save_error = None

            for part in msg.walk():
                content_disposition = part.get_content_disposition() or ""
                if "attachment" not in content_disposition:
                    continue

                found_attachment = True
                print("[email_monitor] Attachment found: True")

                filename = part.get_filename()
                if not filename:
                    continue

                filename = decode_mime_words(filename)
                all_attachment_filenames.append(filename)
                print(f"[email_monitor] Attachment filename: {filename}")
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

                try:
                    dest.write_bytes(part.get_payload(decode=True))
                    print(f"[email_monitor] Attachment saved: True ({dest})")
                    logger.info("Saved attachment: %s", dest)
                    saved_files.append(str(dest))
                except Exception as e:
                    staging_status = "Failed"
                    save_error = str(e)
                    print(f"[email_monitor] Attachment saved: False ({dest})")
                    logger.exception("Failed to save attachment: %s", dest)

            if not found_attachment:
                print("[email_monitor] Attachment found: False")

            # Always create a record — classifier needs it even for no-attachment emails
            print("[email_monitor] Airtable Incoming Documents create queued: True")
            results.append(
                {
                    "sender": sender,
                    "subject": subject,
                    "date_received": date_received,
                    "attachments": saved_files,
                    "all_filenames": all_attachment_filenames,
                    "has_attachments": found_attachment and len(saved_files) > 0,
                    "status": staging_status,
                    "save_error": save_error,
                    "msg": msg,
                }
            )

        except Exception as e:
            print(f"[email_monitor] Exception processing email id {msg_id}: {e}")
            raise

    return results


if __name__ == "__main__":
    server = None
    try:
        print("[email_monitor] Entrypoint: starting connect_imap()")
        server = connect_imap()
        print("[email_monitor] Entrypoint: finished connect_imap()")

        print("[email_monitor] Entrypoint: starting fetch_unread_emails(server)")
        results = fetch_unread_emails(server)
        print("[email_monitor] Entrypoint: finished fetch_unread_emails(server)")

        actionable_results = []
        for entry in results:
            staging_status = entry["status"]
            payload = {
                "Sender Email": entry["sender"],
                "Subject": entry["subject"],
                "Date Received": entry["date_received"],
                "File Names": ", ".join(entry["attachments"]),
                "Status": staging_status,
            }
            print(
                f"[email_monitor][airtable] create target: base={config.AIRTABLE_BASE_ID} table={config.AIRTABLE_TABLE_NAME}"
            )
            print(f"[email_monitor][airtable] create payload: {payload}")
            if entry.get("save_error"):
                print(f"[email_monitor] Attachment save error: {entry['save_error']}")
            try:
                record = create_document_record(
                    sender=entry["sender"],
                    subject=entry["subject"],
                    date_received=entry["date_received"],
                    attachment_paths=entry["attachments"],
                    status=staging_status,
                )
                print("[email_monitor] Airtable Incoming Documents record created: True")
                print(f"[email_monitor][airtable] create response: {record}")
                print(f"[email_monitor][airtable] created record id: {record.get('id')}")

                # ── Email classification ─────────────────────────────────
                record_id = record.get("id")
                classification = classify_email(
                    msg=entry["msg"],
                    sender=entry["sender"],
                    subject=entry["subject"],
                    attachment_filenames=entry.get("all_filenames", []),
                    has_attachments=entry.get("has_attachments", False),
                )
                print(f"[email_monitor] Classification: {classification.event_type} "
                      f"(confidence={classification.confidence}, method={classification.method}, "
                      f"skip={classification.should_skip})")

                if record_id:
                    write_classification_to_airtable(get_table(), record_id, classification)

                if classification.should_skip:
                    print(f"[email_monitor] Email classified as {classification.event_type} — skipping extraction")

                    # ── Bounce vendor lookup ──────────────────────────────
                    if classification.event_type in (EVENT_BOUNCE, EVENT_AUTO_REPLY) and record_id:
                        try:
                            from pyairtable import Api
                            import config as _cfg
                            _api = Api(_cfg.AIRTABLE_API_KEY)
                            _vendors = _api.table(_cfg.AIRTABLE_BASE_ID, "Vendors")
                            _eq = _api.table(_cfg.AIRTABLE_BASE_ID, "Email Queue")
                            handle_bounce(get_table(), record_id, entry["sender"], _vendors, _eq)
                        except Exception as _exc:
                            print(f"[email_monitor] Bounce handler failed: {_exc}")

                    continue

                # ── Cloud link handling ───────────────────────────────────
                if classification.event_type == EVENT_CLOUD_LINK and record_id:
                    try:
                        from pyairtable import Api
                        import config as _cfg
                        _api = Api(_cfg.AIRTABLE_API_KEY)
                        _eq = _api.table(_cfg.AIRTABLE_BASE_ID, "Email Queue")
                        body_text = _extract_body_text(entry["msg"])
                        handle_cloud_link(
                            get_table(), record_id, body_text,
                            entry["sender"], _eq,
                        )
                    except Exception as _exc:
                        print(f"[email_monitor] Cloud link handler failed: {_exc}")

                # ── Attachment size and type gating ───────────────────────
                gated = False
                if entry.get("attachments") and record_id:
                    try:
                        from pyairtable import Api
                        import config as _cfg
                        _api = Api(_cfg.AIRTABLE_API_KEY)
                        _eq = _api.table(_cfg.AIRTABLE_BASE_ID, "Email Queue")

                        for att_path in entry["attachments"]:
                            # Unsupported type check
                            if gate_unsupported_type(
                                get_table(), record_id, att_path, entry["sender"], _eq
                            ):
                                gated = True
                                print(f"[email_monitor] Unsupported file type: {att_path}")
                                break

                            # Size check
                            size_mb = check_attachment_size(att_path)
                            if gate_oversize_attachment(
                                get_table(), record_id, att_path, size_mb, entry["sender"], _eq
                            ):
                                gated = True
                                print(f"[email_monitor] Oversized attachment: {att_path} ({size_mb:.1f}MB)")
                                break
                    except Exception as _exc:
                        print(f"[email_monitor] Attachment gating failed: {_exc}")

                if gated:
                    continue

                # Only pass actionable emails downstream
                entry["event_type"] = classification.event_type
                entry["flag_module_8b"] = classification.flag_module_8b
                actionable_results.append(entry)

            except Exception as e:
                print("[email_monitor] Airtable Incoming Documents record created: False")
                print(
                    f"[email_monitor][airtable] create exception type: {type(e).__module__}.{type(e).__name__}"
                )
                print(f"[email_monitor][airtable] create exception message: {e}")
                raise

        print(f"[email_monitor] Entrypoint: results count = {len(results)} (actionable: {len(actionable_results)})")
    except Exception:
        print("[email_monitor] Entrypoint: exception occurred")
        print(traceback.format_exc())
    finally:
        if server is not None:
            print("[email_monitor] Entrypoint: starting IMAP logout")
            try:
                server.logout()
                print("[email_monitor] Entrypoint: finished IMAP logout")
            except Exception:
                print("[email_monitor] Entrypoint: IMAP logout failed")
                print(traceback.format_exc())
