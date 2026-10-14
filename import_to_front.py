"""
Import split certificate PDFs into Front as individual conversations.

Usage:
    .venv/bin/python import_to_front.py --dry-run [--limit N] [--start N]
    .venv/bin/python import_to_front.py --apply   [--limit N] [--start N]

Hard guarantees (enforced at runtime, not just convention):
  - Only POSTs to POST /inboxes/{id}/imported_messages. No send, reply,
    forward, draft, or delete endpoints are ever called.
  - Refuses to run against any inbox other than inb_k4xex (General Box).
  - sender.handle and every "to" address must end in @carolinacompliancesolutions.com.
  - Before any --apply, GETs the channel list and refuses if sender or to
    address matches any Front channel address (prevents routing to COI@/Compliance@).
  - is_inbound is always True.
  - thread_ref = external_id so every certificate becomes its own conversation.
  - FRONT_API_TOKEN loaded from .env; never printed, logged, or committed.
  - No Airtable writes.
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

import config  # provides FRONT_API_TOKEN via env

# ── Constants ─────────────────────────────────────────────────────────────────

FRONT_API_BASE     = "https://api2.frontapp.com"
ALLOWED_INBOX_ID   = "inb_k4xex"          # General Box — only valid target
REQUIRED_DOMAIN    = "@carolinacompliancesolutions.com"
# rtr-import@ belongs to no Front channel; prevents routing to COI@/Compliance@
SENDER_HANDLE      = "rtr-import@carolinacompliancesolutions.com"
TO_ADDRESS         = "rtr-import@carolinacompliancesolutions.com"
IMPORT_TAGS        = ["RTR"]
EXTERNAL_ID_PREFIX = "ccs-rtr-"

SPLIT_DIR    = Path("/Users/haleybridges/Desktop/RTR Construction LLC/Split Certificates")
INDEX_CSV    = SPLIT_DIR / "index.csv"
LEDGER_PATH  = Path(__file__).parent / "front_imported.json"


# ── Safety guards (raise on violation — tested explicitly) ────────────────────

def _require_inbox(inbox_id: str) -> None:
    if inbox_id != ALLOWED_INBOX_ID:
        raise ValueError(
            f"Refused: inbox '{inbox_id}' is not the allowed General Box "
            f"({ALLOWED_INBOX_ID}). Aborting."
        )


def _require_ccs_domain(label: str, address: str) -> None:
    if not address.lower().endswith(REQUIRED_DOMAIN):
        raise ValueError(
            f"Refused: {label} '{address}' does not end in "
            f"'{REQUIRED_DOMAIN}'. Aborting."
        )


def _validate_addresses(sender_handle: str, to_addresses: list[str]) -> None:
    _require_ccs_domain("sender.handle", sender_handle)
    for addr in to_addresses:
        _require_ccs_domain("to address", addr)


def fetch_channel_addresses(token: str) -> set[str]:
    """GET /channels and return the set of channel addresses (lowercased)."""
    import requests
    resp = requests.get(
        f"{FRONT_API_BASE}/channels",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Could not fetch Front channels: {resp.status_code} {resp.text[:200]}")
    data = resp.json()
    return {
        ch.get("address", "").lower()
        for ch in data.get("_results", [])
        if ch.get("address")
    }


def _require_not_a_channel(addresses: list[str], token: str) -> None:
    """Refuse if any address in `addresses` matches a Front channel address."""
    channel_addrs = fetch_channel_addresses(token)
    for addr in addresses:
        if addr.lower() in channel_addrs:
            raise ValueError(
                f"Refused: '{addr}' is a Front channel address "
                f"(channels: {sorted(channel_addrs)}). "
                f"Use an address that belongs to no Front channel. Aborting."
            )


# ── Index loading + deduplication ─────────────────────────────────────────────

def load_eligible_rows(index_csv: Path = INDEX_CSV) -> list[dict]:
    """Read index.csv; exclude non-cert rows; deduplicate by sha256 (keep first).

    Rows are sorted by (source_filename, page_number) so that COI forms.PDF
    pages appear before COI forms1.PDF pages (because '.' < '1' in ASCII),
    matching the user's 1-based --start indexing expectations.
    """
    if not index_csv.exists():
        raise FileNotFoundError(f"index.csv not found: {index_csv}")

    rows = list(csv.DictReader(open(index_csv)))
    # Sort by source file then page so COI forms.PDF (p01…p47) comes before
    # COI forms1.PDF (p01…p08) — '.' (46) < '1' (49) in the filename.
    rows.sort(key=lambda r: (r["source_filename"], int(r["page_number"])))

    seen_sha: dict[str, str] = {}
    eligible: list[dict] = []

    for row in rows:
        if (row.get("is_non_cert") or "").lower() == "true":
            print(f"  [SKIP non-cert] {row['split_filename']}")
            continue
        sha = row["sha256"]
        if sha in seen_sha:
            print(f"  [SKIP duplicate] {row['split_filename']} "
                  f"(same sha256 as {seen_sha[sha]})")
            continue
        seen_sha[sha] = row["split_filename"]
        eligible.append(row)

    return eligible


# ── Ledger (local dedup across runs) ─────────────────────────────────────────

def load_ledger(path: Path = LEDGER_PATH) -> dict[str, dict]:
    """Return {external_id: front_response_dict} from the local ledger file."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_ledger(ledger: dict, path: Path = LEDGER_PATH) -> None:
    path.write_text(json.dumps(ledger, indent=2))


# ── Message construction ──────────────────────────────────────────────────────

def build_message(row: dict) -> dict:
    """Return a dict describing one import payload (no HTTP yet)."""
    stem       = Path(row["split_filename"]).stem   # e.g. COI_forms_p01
    subject    = f"Certificate - {stem}"
    # Body contains only structural metadata — no vendor names
    body = (
        f"Source file: {row['source_filename']}  |  "
        f"Page {row['page_number']} of {row['page_count_of_source']}  |  "
        f"Folder: {row['source_folder']}"
    )
    external_id = EXTERNAL_ID_PREFIX + row["sha256"]

    return {
        "split_filename": row["split_filename"],
        "pdf_path":       SPLIT_DIR / row["split_filename"],
        "inbox_id":       ALLOWED_INBOX_ID,
        "sender_handle":  SENDER_HANDLE,
        "to":             [TO_ADDRESS],
        "type":           "email",
        "subject":        subject,
        "body":           body,
        "external_id":    external_id,
        "created_at":     int(time.time()),
        "tags":           IMPORT_TAGS,
        "metadata": {
            "is_inbound":        True,
            "should_skip_rules": True,
        },
    }


# ── Front API: import one message ─────────────────────────────────────────────

def import_message(msg: dict, token: str) -> dict:
    """POST one message to Front's import endpoint. Returns the parsed response."""
    import requests

    _require_inbox(msg["inbox_id"])
    _validate_addresses(msg["sender_handle"], msg["to"])

    url = f"{FRONT_API_BASE}/inboxes/{msg['inbox_id']}/imported_messages"
    headers = {"Authorization": f"Bearer {token}"}

    pdf_path: Path = msg["pdf_path"]
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    # Multipart form-data — bracket notation for nested fields.
    # thread_ref = external_id ensures every certificate becomes its own
    # conversation (without it Front groups messages by sender+recipient).
    data = {
        "sender[handle]":              msg["sender_handle"],
        "to[0]":                       msg["to"][0],
        "type":                        msg["type"],
        "subject":                     msg["subject"],
        "body":                        msg["body"],
        "external_id":                 msg["external_id"],
        "created_at":                  str(msg["created_at"]),
        "metadata[is_inbound]":        "true",
        "metadata[should_skip_rules]": "true",
        "metadata[thread_ref]":        msg["external_id"],
        "tags[0]":                     msg["tags"][0],
    }
    with open(pdf_path, "rb") as fh:
        pdf_bytes = fh.read()

    files = {
        "attachments": (pdf_path.name, pdf_bytes, "application/pdf"),
    }

    resp = requests.post(url, headers=headers, data=data, files=files, timeout=30)

    if resp.status_code not in (200, 202):
        raise RuntimeError(
            f"Front API error {resp.status_code}: {resp.text[:500]}"
        )

    return resp.json() if resp.text.strip() else {}


# ── Front API: post-import verification ──────────────────────────────────────

def verify_imports(imported_results: list[dict], token: str) -> None:
    """GET each created conversation and report threading, inbox, tags, attachments.

    Searches General Box for conversations matching our import subjects,
    then GETs each conversation to verify isolation, tags, and attachment.
    """
    import requests
    import time as _time

    headers = {"Authorization": f"Bearer {token}"}

    subjects_to_check = {
        r["split_filename"]: f"Certificate - {Path(r['split_filename']).stem}"
        for r in imported_results
        if r.get("status") == "imported"
    }
    if not subjects_to_check:
        print("\n[VERIFY] No imported results to verify.")
        return

    print(f"\n[VERIFY] Waiting 3 s for Front to process imports…")
    _time.sleep(3)

    # GET recent conversations in General Box
    url = f"{FRONT_API_BASE}/inboxes/{ALLOWED_INBOX_ID}/conversations"
    resp = requests.get(url, headers=headers, params={"limit": 25}, timeout=15)
    if resp.status_code != 200:
        print(f"[VERIFY] Could not list inbox conversations: {resp.status_code}")
        return

    convs = resp.json().get("_results", [])
    found: dict[str, dict] = {}
    for conv in convs:
        subj = conv.get("subject") or ""
        for filename, expected_subj in subjects_to_check.items():
            if subj == expected_subj:
                found[filename] = conv
                break

    print(f"\n[VERIFY] Found {len(found)}/{len(subjects_to_check)} conversation(s) in General Box")

    conv_ids_seen: list[str] = []

    for filename, expected_subj in subjects_to_check.items():
        print(f"\n  ── {filename} ──")
        conv = found.get(filename)
        if not conv:
            print(f"    NOT FOUND in General Box (may still be processing)")
            continue

        conv_id = conv.get("id", "?")
        conv_ids_seen.append(conv_id)

        # GET full conversation details
        # GET full conversation detail
        detail_resp = requests.get(
            f"{FRONT_API_BASE}/conversations/{conv_id}",
            headers=headers, timeout=10
        )
        detail = detail_resp.json() if detail_resp.status_code == 200 else conv

        # GET inbox membership (not embedded in conversation detail)
        inboxes_resp = requests.get(
            f"{FRONT_API_BASE}/conversations/{conv_id}/inboxes",
            headers=headers, timeout=10
        )
        if inboxes_resp.status_code == 200:
            inboxes = inboxes_resp.json().get("_results", [])
        else:
            inboxes = []

        # (a) Conversation ID and status
        print(f"    conversation_id: {conv_id}  (status: {detail.get('status','?')})")

        # (b) Inboxes — must be exactly General Box
        inbox_names = [i.get("name", i.get("id", "?")) for i in inboxes]
        inbox_ids   = [i.get("id", "?") for i in inboxes]
        in_general_only = (len(inbox_ids) == 1 and inbox_ids[0] == ALLOWED_INBOX_ID)
        print(f"    inboxes: {inbox_names}  "
              f"{'✓ General Box only' if in_general_only else '✗ UNEXPECTED INBOXES: ' + str(inbox_names)}")

        # (c) Tags
        tags_on_conv = [t.get("name", "?") for t in (detail.get("tags") or [])]
        has_rtr = "RTR" in tags_on_conv
        print(f"    tags: {tags_on_conv}  {'✓' if has_rtr else '✗ RTR tag missing'}")

        # (d) ticket_ids — Front's own sequential conversation reference numbers (SU-N).
        #     These are NOT external tickets; every conversation in a Front workspace
        #     gets one. No auto-routing to a helpdesk occurred.
        ticket_ids = detail.get("ticket_ids") or []
        print(f"    ticket_ids (Front seq refs): {ticket_ids}  "
              f"(these are Front's internal conversation reference numbers, not external tickets)")

        # (e) Attachment — check latest message
        msgs_resp = requests.get(
            f"{FRONT_API_BASE}/conversations/{conv_id}/messages",
            headers=headers, timeout=10
        )
        if msgs_resp.status_code == 200:
            msgs = msgs_resp.json().get("_results", [])
            attachments = []
            for m in msgs:
                attachments.extend(m.get("attachments") or [])
            att_names = [a.get("filename", "?") for a in attachments]
            print(f"    attachments: {att_names}  "
                  f"{'✓' if att_names else '✗ no attachments found'}")
        else:
            print(f"    attachments: could not retrieve ({msgs_resp.status_code})")

    # (a) Are both in different conversations?
    if len(conv_ids_seen) == len(subjects_to_check):
        unique = len(set(conv_ids_seen)) == len(conv_ids_seen)
        print(f"\n  conversation isolation: "
              f"{'✓ each certificate is its own conversation' if unique else '✗ SHARED CONVERSATION — ids: ' + str(conv_ids_seen)}")
    else:
        print(f"\n  conversation isolation: could not verify "
              f"(only {len(conv_ids_seen)}/{len(subjects_to_check)} conversations found)")


# ── Main runner ───────────────────────────────────────────────────────────────

def run(
    apply: bool,
    limit: Optional[int],
    token: str,
    index_csv: Path = INDEX_CSV,
    ledger_path: Path = LEDGER_PATH,
    inbox_id: str = ALLOWED_INBOX_ID,
    files_filter: Optional[list[str]] = None,
    start: Optional[int] = None,
) -> list[dict]:
    """Execute the import run. Returns list of result dicts (one per row processed)."""

    # Guard: refuse wrong inbox before touching anything
    _require_inbox(inbox_id)
    _validate_addresses(SENDER_HANDLE, [TO_ADDRESS])

    # Channel preflight — only on apply (dry run never calls Front)
    if apply:
        print("Pre-flight: checking that import addresses are not Front channels…")
        _require_not_a_channel([SENDER_HANDLE, TO_ADDRESS], token)
        print(f"  ✓ {SENDER_HANDLE} is not a channel address")

    eligible = load_eligible_rows(index_csv)
    ledger   = load_ledger(ledger_path)

    if files_filter is not None:
        allowed = set(files_filter)
        eligible = [r for r in eligible if r["split_filename"] in allowed]

    # --start N: 1-based, skip the first N-1 eligible rows
    if start is not None and start > 1:
        eligible = eligible[start - 1:]

    if limit is not None:
        eligible = eligible[:limit]

    results: list[dict] = []

    for row in eligible:
        msg = build_message(row)
        ext_id = msg["external_id"]

        if ext_id in ledger:
            prev = ledger[ext_id]
            uid = prev.get("message_uid", "?") if isinstance(prev, dict) else str(prev)[:40]
            print(f"  [SKIP ledger] {row['split_filename']} "
                  f"→ already imported, message_uid={uid}")
            results.append({
                "split_filename": row["split_filename"],
                "status": "ledger_skip",
                "front_response": prev,
            })
            continue

        if apply:
            try:
                resp = import_message(msg, token)
                ledger[ext_id] = resp
                save_ledger(ledger, ledger_path)
                print(f"  [IMPORTED] {row['split_filename']} "
                      f"→ {resp.get('message_uid','?')} "
                      f"(status: {resp.get('status','?')})")
                results.append({
                    "split_filename": row["split_filename"],
                    "status": "imported",
                    "front_response": resp,
                })
            except Exception as exc:
                print(f"  [FAILED] {row['split_filename']}: {exc}")
                results.append({
                    "split_filename": row["split_filename"],
                    "status": "failed",
                    "error": str(exc),
                })
        else:
            print(
                f"  [DRY RUN] {row['split_filename']}\n"
                f"    inbox:       {msg['inbox_id']}\n"
                f"    subject:     {msg['subject']}\n"
                f"    sender:      {msg['sender_handle']}\n"
                f"    to:          {msg['to']}\n"
                f"    attachment:  {Path(msg['pdf_path']).name}\n"
                f"    tags:        {msg['tags']}\n"
                f"    external_id: {ext_id}\n"
                f"    is_inbound:  {msg['metadata']['is_inbound']}\n"
                f"    thread_ref:  {ext_id}\n"
                f"    body:        {msg['body']}"
            )
            results.append({
                "split_filename": row["split_filename"],
                "status": "dry_run",
                "msg": msg,
            })

    mode = "APPLY" if apply else "DRY RUN"
    print(f"\n{mode} complete: {len(results)} row(s) processed.")

    if apply:
        verify_imports(results, token)

    return results


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", dest="dry_run",
                       help="Print what would be sent; call nothing (default safe mode)")
    group.add_argument("--apply",   action="store_true",
                       help="Actually POST to Front's import endpoint")
    ap.add_argument("--limit", type=int, default=None,
                    help="Process at most N rows (applied after dedup)")
    ap.add_argument("--start", type=int, default=None,
                    help="1-based start offset: skip the first N-1 eligible rows "
                         "(e.g. --start 3 skips rows 1-2)")
    ap.add_argument("--files", type=str, default=None,
                    help="Comma-separated list of split_filenames to import "
                         "(e.g. COI_forms_p01.pdf,COI_forms_p02.pdf)")
    args = ap.parse_args()

    token = (getattr(config, "FRONT_API_TOKEN", None) or "").strip()
    if not token:
        sys.exit("FRONT_API_TOKEN is not set in .env")

    files_filter: Optional[list[str]] = None
    if args.files:
        files_filter = [f.strip() for f in args.files.split(",") if f.strip()]

    run(
        apply=args.apply,
        limit=args.limit,
        start=args.start,
        token=token,
        files_filter=files_filter,
    )


if __name__ == "__main__":
    main()
