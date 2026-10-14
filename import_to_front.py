"""
Import split certificate PDFs into Front as individual conversations.

Usage:
    .venv/bin/python import_to_front.py --dry-run [--limit N]
    .venv/bin/python import_to_front.py --apply   [--limit N]

Hard guarantees (enforced at runtime, not just convention):
  - Only POSTs to POST /inboxes/{id}/imported_messages. No send, reply,
    forward, draft, or delete endpoints are ever called.
  - Refuses to run against any inbox other than inb_k4xex (General Box).
  - sender.handle and every "to" address must end in @carolinacompliancesolutions.com.
  - is_inbound is always True.
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
SENDER_HANDLE      = "coi-intake@carolinacompliancesolutions.com"
TO_ADDRESS         = "compliance@carolinacompliancesolutions.com"
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


# ── Index loading + deduplication ─────────────────────────────────────────────

def load_eligible_rows(index_csv: Path = INDEX_CSV) -> list[dict]:
    """Read index.csv; exclude non-cert rows; deduplicate by sha256 (keep first).

    Returns list of dicts, one per certificate page to import.
    """
    if not index_csv.exists():
        raise FileNotFoundError(f"index.csv not found: {index_csv}")

    rows = list(csv.DictReader(open(index_csv)))
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

def load_ledger(path: Path = LEDGER_PATH) -> dict[str, str]:
    """Return {external_id: conversation_id} from the local ledger file."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_ledger(ledger: dict[str, str], path: Path = LEDGER_PATH) -> None:
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


# ── Front API call (one import) ───────────────────────────────────────────────

def import_message(msg: dict, token: str) -> dict:
    """POST one message to Front's import endpoint. Returns the parsed response."""
    import requests  # local import keeps the top-level importable without requests

    _require_inbox(msg["inbox_id"])
    _validate_addresses(msg["sender_handle"], msg["to"])

    url = f"{FRONT_API_BASE}/inboxes/{msg['inbox_id']}/imported_messages"
    headers = {"Authorization": f"Bearer {token}"}

    pdf_path: Path = msg["pdf_path"]
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    # Multipart form-data — bracket notation for nested fields.
    # thread_ref = external_id ensures every certificate becomes its own
    # conversation (without it Front groups messages by sender).
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

    body = resp.json() if resp.text.strip() else {}
    return body


# ── Main runner ───────────────────────────────────────────────────────────────

def run(
    apply: bool,
    limit: Optional[int],
    token: str,
    index_csv: Path = INDEX_CSV,
    ledger_path: Path = LEDGER_PATH,
    inbox_id: str = ALLOWED_INBOX_ID,
    files_filter: Optional[list[str]] = None,
) -> list[dict]:
    """Execute the import run. Returns list of result dicts (one per row processed)."""

    # Guard: refuse wrong inbox before touching anything
    _require_inbox(inbox_id)
    _validate_addresses(SENDER_HANDLE, [TO_ADDRESS])

    eligible = load_eligible_rows(index_csv)
    ledger   = load_ledger(ledger_path)

    if files_filter is not None:
        allowed = set(files_filter)
        eligible = [r for r in eligible if r["split_filename"] in allowed]

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
                # Front returns {status, message_uid, conversation_reference}
                # Store the full response so re-runs can look it up.
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
            # Dry run — print exactly what would be sent, call nothing
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
                f"    body:        {msg['body']}"
            )
            results.append({
                "split_filename": row["split_filename"],
                "status": "dry_run",
                "msg": msg,
            })

    mode = "APPLY" if apply else "DRY RUN"
    print(f"\n{mode} complete: {len(results)} row(s) processed.")
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
        token=token,
        files_filter=files_filter,
    )


if __name__ == "__main__":
    main()
