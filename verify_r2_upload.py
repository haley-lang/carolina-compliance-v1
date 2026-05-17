"""Verify the N most recent Incoming Documents have their PDFs persisted in R2.

For each record (newest first), reads the `PDF R2 Key` field from Airtable and
calls boto3 client.head_object() against each comma-separated R2 key to
confirm the object actually exists in the bucket. Surfaces three states per
record:

    OK      - object exists in R2; reports size, content-type, last-modified
    MISSING - Airtable says the key exists, but R2 head_object 404s
    BLANK   - record has no PDF R2 Key (predates 1B, or upload failed)

Companion to inspect_recent_extractions.py — that one looks at Incoming
Extractions + confidence; this one looks at Incoming Documents + R2.

Usage:
    .venv/bin/python verify_r2_upload.py        # default 5 records
    .venv/bin/python verify_r2_upload.py 10
"""

import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

from pyairtable import Api  # noqa: E402

import config  # noqa: E402
from r2_storage import get_r2_client  # noqa: E402


def main(n: int = 5) -> None:
    api = Api(config.AIRTABLE_API_KEY)
    table = api.table(config.AIRTABLE_BASE_ID, config.AIRTABLE_TABLE_NAME)
    records = table.all(
        fields=["Date Received", "Sender Email", "PDF R2 Key", "File Names"],
        sort=["-Date Received"],
        max_records=n,
    )

    r2_client, r2_bucket = get_r2_client()
    if r2_client is None:
        print("[warn] R2 client unavailable — missing env vars. "
              "Showing Airtable side only; head_object checks skipped.\n")

    sep = "-" * 78
    for r in records:
        rec_id = r["id"]
        fields = r["fields"]
        date_received = (fields.get("Date Received") or "")[:19]
        sender = (fields.get("Sender Email") or "")[:48]
        r2_key_field = (fields.get("PDF R2 Key") or "").strip()

        print(sep)
        print(f"Record    : {rec_id}")
        print(f"Date      : {date_received}")
        print(f"Sender    : {sender}")

        if not r2_key_field:
            print("R2 Key    : BLANK (record predates 1B, or R2 upload failed at intake)")
            continue

        keys = [k.strip() for k in r2_key_field.split(",") if k.strip()]
        for key in keys:
            print(f"R2 Key    : {key}")
            if r2_client is None:
                print("            (head_object skipped — no R2 client)")
                continue
            try:
                head = r2_client.head_object(Bucket=r2_bucket, Key=key)
                size = head.get("ContentLength")
                ctype = head.get("ContentType") or "?"
                lm = head.get("LastModified")
                lm_str = lm.isoformat() if hasattr(lm, "isoformat") else str(lm or "?")
                size_str = f"{size:,}B" if isinstance(size, int) else "?"
                print(f"            OK      size={size_str}  type={ctype}  modified={lm_str}")
            except Exception as exc:
                err_code = ""
                try:
                    err_code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
                except Exception:
                    pass
                if err_code in ("404", "NoSuchKey", "NotFound"):
                    print("            MISSING (Airtable claims key exists; R2 head_object 404)")
                else:
                    print(f"            ERROR   head_object failed: {exc}")
    print(sep)


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    main(n)
