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
    handle_bounce, handle_cloud_link, handle_no_attachment,
    gate_oversize_attachment, gate_unsupported_type, check_attachment_size,
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
    logger.info("[email_monitor][path-debug] cwd=%s", os.getcwd())
    logger.info("[email_monitor][path-debug] config.UPLOAD_DIR=%r", config.UPLOAD_DIR)
    logger.info("[email_monitor][path-debug] upload_dir.resolve()=%s", upload_dir.resolve())

    results = []

    for msg_id in message_ids:
        try:
            # Use BODY.PEEK[] not RFC822 — RFC822 implicitly marks the message
            # \Seen on fetch (RFC 3501 side effect), which silently loses any
            # email that crashes between fetch and Airtable write. With PEEK,
            # the message stays UNSEEN until we explicitly add_flags after the
            # Airtable record is confirmed created.
            raw = server.fetch([msg_id], ["BODY.PEEK[]"])
            msg = email.message_from_bytes(raw[msg_id][b"BODY[]"])

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
                    payload = part.get_payload(decode=True)
                    payload_len = len(payload) if payload is not None else 0
                    logger.info(
                        "[email_monitor][write] About to write %d bytes to %s",
                        payload_len, dest.resolve(),
                    )
                    dest.write_bytes(payload)
                    exists = dest.exists()
                    size = dest.stat().st_size if exists else None
                    logger.info(
                        "[email_monitor][write] Write completed. dest=%s exists=%s size=%s",
                        dest.resolve(), exists, size,
                    )
                    print(f"[email_monitor] Attachment saved: True ({dest})")
                    saved_files.append(str(dest))
                except Exception as e:
                    staging_status = "Failed"
                    save_error = str(e)
                    logger.error(
                        "[email_monitor][write] Write FAILED for %s: %s: %s",
                        dest.resolve() if dest else "<unresolved>", type(e).__name__, e,
                        exc_info=True,
                    )
                    print(f"[email_monitor] Attachment saved: False ({dest})")

            if not found_attachment:
                print("[email_monitor] Attachment found: False")

            # Always create a record — classifier needs it even for no-attachment emails
            print("[email_monitor] Airtable Incoming Documents create queued: True")
            results.append(
                {
                    "msg_id": msg_id,
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

                # ── Mark message Seen on Gmail (dedup hygiene) ──────────
                # The Airtable record is now the source of truth; the Seen
                # flag prevents this message from being re-fetched on the
                # next cron run. If marking fails (transient IMAP error), a
                # duplicate Airtable record may be created next run — log and
                # continue, don't unwind the successful Airtable write.
                try:
                    server.add_flags(entry["msg_id"], [b"\\Seen"])
                    print(f"[email_monitor] Marked Seen: msg_id={entry['msg_id']}")
                except Exception as _seen_exc:
                    print(f"[email_monitor] add_flags failed for msg_id={entry['msg_id']}: {_seen_exc}")

                # ── Move message to "Processed" Gmail label ─────────────
                # Keeps INBOX clean as volume grows. Gmail treats labels as
                # folders for IMAP: copy adds the Processed label, then
                # delete+expunge removes the INBOX label (the message itself
                # persists under Processed). The "Processed" label must exist
                # in Gmail; if not, copy raises and the message stays in INBOX
                # (still Seen — no re-fetch, no data loss).
                #
                # Wrapped in try/except per the same source-of-truth principle
                # as the Seen flag above: an IMAP move failure must not unwind
                # the successful Airtable write.
                try:
                    server.copy([entry["msg_id"]], "Processed")
                    server.delete_messages([entry["msg_id"]])
                    server.expunge()
                    print(f"[email_monitor] Moved to Processed: msg_id={entry['msg_id']}")
                except Exception as _move_exc:
                    print(f"[email_monitor] Move to Processed failed for msg_id={entry['msg_id']}: {_move_exc}")

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

                # ── Follow-up ladder: passive response detection ────────
                # Runs on EVERY inbound email. If the sender and body match
                # an Active ladder's named insured, that ladder is halted.
                try:
                    _body_text_for_ladder = _extract_body_text(entry["msg"]) or ""
                    from module_22_followup_ladder import (
                        check_response_match, terminate_ladder,
                        STAGE_RESPONDED,
                    )
                    _matched_ladder = check_response_match(
                        inbound_sender=entry["sender"],
                        inbound_subject=entry["subject"],
                        inbound_body=_body_text_for_ladder,
                    )
                    if _matched_ladder:
                        terminate_ladder(
                            _matched_ladder,
                            new_stage=STAGE_RESPONDED,
                            reason=f"Reply from {entry['sender']} referenced named insured",
                        )
                        print(f"[email_monitor] Ladder {_matched_ladder} halted by response from {entry['sender']}")
                except Exception as _exc:
                    print(f"[email_monitor] Ladder response check failed: {_exc}")

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

                        # Follow-up ladder bounce handling (retry to PDF
                        # contact or escalate, depending on Bounce Count).
                        try:
                            from module_22_followup_ladder import (
                                handle_bounce as ladder_handle_bounce,
                            )
                            ladder_handle_bounce(bounced_address=entry["sender"])
                        except Exception as _exc:
                            print(f"[email_monitor] Ladder bounce handler failed: {_exc}")

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

                # ── No attachment check ───────────────────────────────
                if not entry.get("has_attachments") and record_id:
                    try:
                        from pyairtable import Api
                        import config as _cfg
                        _api = Api(_cfg.AIRTABLE_API_KEY)
                        _eq = _api.table(_cfg.AIRTABLE_BASE_ID, "Email Queue")
                        handle_no_attachment(get_table(), record_id, entry["sender"], _eq)
                    except Exception as _exc:
                        print(f"[email_monitor] No attachment handler failed: {_exc}")
                    continue

                # ── Per-attachment size and type gating ───────────────
                # Each attachment checked independently — failure on one
                # does not block the others from processing.
                passing_attachments = []
                if entry.get("attachments") and record_id:
                    try:
                        from pyairtable import Api
                        import config as _cfg
                        _api = Api(_cfg.AIRTABLE_API_KEY)
                        _eq = _api.table(_cfg.AIRTABLE_BASE_ID, "Email Queue")

                        for att_path in entry["attachments"]:
                            if gate_unsupported_type(
                                get_table(), record_id, att_path, entry["sender"], _eq
                            ):
                                print(f"[email_monitor] Unsupported file type (skipped): {att_path}")
                                continue

                            size_mb = check_attachment_size(att_path)
                            if gate_oversize_attachment(
                                get_table(), record_id, att_path, size_mb, entry["sender"], _eq
                            ):
                                print(f"[email_monitor] Oversized attachment (skipped): {att_path} ({size_mb:.1f}MB)")
                                continue

                            passing_attachments.append(att_path)
                    except Exception as _exc:
                        print(f"[email_monitor] Attachment gating failed: {_exc}")
                        passing_attachments = entry["attachments"]

                if not passing_attachments:
                    print("[email_monitor] All attachments failed gating — skipping email")
                    continue

                entry["attachments"] = passing_attachments

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
