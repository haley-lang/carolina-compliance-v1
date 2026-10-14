"""
Split Billy/RTR source PDFs into one-page files, write index.csv,
and do a read-only Airtable match check.

Usage:
    .venv/bin/python split_billy_certs.py          # split + index + airtable check
    .venv/bin/python split_billy_certs.py --verify  # verify only (no splits written)
"""
import argparse
import csv
import hashlib
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import config
from airtable_importer import INCOMING_EXTRACTIONS_TABLE, clean_base_id

# ── Paths ─────────────────────────────────────────────────────────────────────
ORIG_DIR   = Path("/Users/haleybridges/Desktop/RTR Construction LLC/Original Certificates")
RESEND_DIR = Path("/Users/haleybridges/Desktop/RTR Construction LLC/To Resend")
OUT_DIR    = Path("/Users/haleybridges/Desktop/RTR Construction LLC/Split Certificates")
INDEX_PATH = OUT_DIR / "index.csv"

SOURCES = [
    (ORIG_DIR,   "Original Certificates"),
    (RESEND_DIR, "To Resend"),
]

# PDFs that are not certificates (application forms, letters, etc.)
# These are still split; they get flagged in the index.
NON_CERT_STEMS = {
    "N9WC394833-ACORDAPP25-I",  # ACORD application form, not a COI
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_stem(stem: str) -> str:
    """Spaces → underscores; keep other characters as-is (no lowercasing)."""
    return stem.replace(" ", "_")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def page_has_text(pdf_path: Path) -> bool:
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                if len((page.extract_text() or "").strip()) >= 20:
                    return True
    except Exception:
        pass
    return False


def verify_split(path: Path) -> bool:
    """Return True if the file opens and has exactly one page."""
    from pypdf import PdfReader
    try:
        return len(PdfReader(str(path)).pages) == 1
    except Exception:
        return False


# ── Step 1+2: Split and index ─────────────────────────────────────────────────

def split_and_index(dry_run: bool = False) -> list[dict]:
    from pypdf import PdfReader, PdfWriter

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing index so we can skip already-written files
    existing_splits: set[str] = set()
    if INDEX_PATH.exists():
        with open(INDEX_PATH, newline="") as f:
            for row in csv.DictReader(f):
                existing_splits.add(row["split_filename"])

    index_rows: list[dict] = []
    skipped = written = 0

    for source_dir, folder_label in SOURCES:
        pdfs = sorted(
            p for p in source_dir.iterdir()
            if p.suffix.lower() == ".pdf"
        )
        print(f"\n{folder_label}: {len(pdfs)} PDF(s)")
        for pdf_path in pdfs:
            reader = PdfReader(str(pdf_path))
            page_count = len(reader.pages)
            stem = safe_stem(pdf_path.stem)
            is_non_cert = pdf_path.stem in NON_CERT_STEMS
            pad = max(2, len(str(page_count)))  # e.g. 47 pages → 2-digit padding

            print(f"  {pdf_path.name}: {page_count} page(s)"
                  + (" [NON-CERT: application form]" if is_non_cert else ""))

            for page_num in range(1, page_count + 1):
                split_name = f"{stem}_p{page_num:0{pad}d}.pdf"
                split_path = OUT_DIR / split_name

                if split_name in existing_splits:
                    print(f"    SKIP (exists): {split_name}")
                    skipped += 1
                else:
                    if not dry_run:
                        writer = PdfWriter()
                        writer.add_page(reader.pages[page_num - 1])
                        with open(split_path, "wb") as fh:
                            writer.write(fh)
                        print(f"    Written: {split_name}")
                        written += 1
                    else:
                        print(f"    WOULD WRITE: {split_name}")

                if not dry_run and split_path.exists():
                    index_rows.append({
                        "split_filename":       split_name,
                        "source_folder":        folder_label,
                        "source_filename":      pdf_path.name,
                        "page_number":          page_num,
                        "page_count_of_source": page_count,
                        "sha256":               sha256(split_path),
                        "has_text_layer":       str(page_has_text(split_path)).lower(),
                        "is_non_cert":          str(is_non_cert).lower(),
                    })

    print(f"\nSplit summary: {written} written, {skipped} skipped (already existed).")

    if not dry_run:
        # Write full index (all files that exist, not just newly written ones)
        # Merge with any existing rows for files we skipped
        merged: dict[str, dict] = {}
        if INDEX_PATH.exists():
            with open(INDEX_PATH, newline="") as f:
                for row in csv.DictReader(f):
                    merged[row["split_filename"]] = row
        for row in index_rows:
            merged[row["split_filename"]] = row

        fieldnames = ["split_filename", "source_folder", "source_filename",
                      "page_number", "page_count_of_source", "sha256",
                      "has_text_layer", "is_non_cert"]
        with open(INDEX_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(sorted(merged.values(), key=lambda r: r["split_filename"]))
        print(f"index.csv: {len(merged)} total rows.")

    return index_rows


# ── Step 3: Airtable read-only match check ────────────────────────────────────

def _possible_airtable_names(split_name: str, source_filename: str, page_num: int, page_count: int) -> list[str]:
    """Return candidate Airtable Source Filenames for this split file.

    Airtable Source Filename is the .json output from the extractor,
    which is named after the PDF cert file that the extractor received.

    Naming conventions observed in this dataset:
      - Multi-page scanned bundles split by pdf_bundle:
            COI forms.PDF → COI forms_cert01.pdf → extractor → COI_forms_cert01.json
        (pdf_bundle uses the original stem including spaces)
        But the JSON file on disk uses whatever filename was passed to the extractor.
        The importer stores json_path.name as Source Filename.
        Experimentally the Airtable rows have names like "COI_forms_cert01.json".

      - Single-page originals:
            coi.PDF stays as coi.pdf → extractor → coi.json

      - Multi-page non-scanned (ACORD app):
            N9WC394833-ACORDAPP25-I.pdf → treated as one cert → N9WC394833-ACORDAPP25-I.json
    """
    src_stem = Path(source_filename).stem   # e.g. "COI forms"
    safe_src_stem = safe_stem(src_stem)     # e.g. "COI_forms"
    pad = max(2, len(str(page_count)))

    candidates = []

    # Single-page original → stem.json
    if page_count == 1:
        candidates.append(f"{safe_src_stem}.json")
        candidates.append(f"{src_stem}.json")

    # Multi-page bundle split by pdf_bundle: cert numbering = page number
    cert_num = f"{page_num:0{pad}d}"
    candidates.append(f"{safe_src_stem}_cert{cert_num}.json")
    # pdf_bundle uses original stem with spaces replaced by underscores in its output
    candidates.append(f"{src_stem.replace(' ', '_')}_cert{cert_num}.json")

    # Resend variants observed in this dataset
    if "resend" in safe_src_stem.lower() and page_count > 1:
        # RTR_COI_forms1_resend_cert01.json
        candidates.append(f"{safe_src_stem}_cert{cert_num}.json")

    # Multi-page non-cert (all pages → same Airtable row)
    if Path(source_filename).stem in NON_CERT_STEMS:
        candidates.append(f"{safe_src_stem}.json")

    return list(dict.fromkeys(candidates))  # dedupe, preserve order


def airtable_check() -> None:
    if not INDEX_PATH.exists():
        print("index.csv not found — run without --verify first.")
        return

    from pyairtable import Api

    token = (config.AIRTABLE_API_KEY or "").strip()
    base_id = clean_base_id(config.AIRTABLE_BASE_ID or "")
    if not token or not base_id:
        print("AIRTABLE_API_KEY or AIRTABLE_BASE_ID missing — skipping Airtable check.")
        return

    api   = Api(token)
    table = api.table(base_id, INCOMING_EXTRACTIONS_TABLE)

    print("\nFetching Airtable Incoming Extractions (Source Filename + Source Page only)…")
    all_records = table.all(fields=["Source Filename", "Source Page"])
    print(f"  {len(all_records)} record(s) fetched.")

    # Build lookup: source_filename_lower → list of record IDs
    from collections import defaultdict
    at_index: dict[str, list[str]] = defaultdict(list)
    for rec in all_records:
        sf = (rec.get("fields") or {}).get("Source Filename") or ""
        if sf:
            at_index[sf.strip().lower()].append(rec["id"])

    with open(INDEX_PATH, newline="") as f:
        index_rows = list(csv.DictReader(f))

    no_match: list[str] = []
    multi_match: list[tuple[str, list[str]]] = []
    matched: list[str] = []

    for row in index_rows:
        split_name = row["split_filename"]
        candidates = _possible_airtable_names(
            split_name,
            row["source_filename"],
            int(row["page_number"]),
            int(row["page_count_of_source"]),
        )
        found_ids: list[str] = []
        matched_name: str = ""
        for cand in candidates:
            ids = at_index.get(cand.lower(), [])
            if ids:
                found_ids = ids
                matched_name = cand
                break

        if not found_ids:
            no_match.append(split_name)
        elif len(found_ids) == 1:
            matched.append(f"{split_name} → {matched_name} ({found_ids[0]})")
        else:
            multi_match.append((split_name, found_ids))

    print(f"\nAirtable match results:")
    print(f"  Matched (1 row):      {len(matched)}")
    print(f"  No match:             {len(no_match)}")
    print(f"  Multiple matches:     {len(multi_match)}")

    print("\n── MATCHED ──────────────────────────────────────────────────────")
    for m in matched:
        print(f"  {m}")

    if no_match:
        print("\n── NO MATCH (no Airtable row found) ─────────────────────────────")
        for name in no_match:
            print(f"  {name}")

    if multi_match:
        print("\n── MULTIPLE MATCHES (ambiguous) ─────────────────────────────────")
        for name, ids in multi_match:
            print(f"  {name}: {ids}")


# ── Step 4: Self-verification ─────────────────────────────────────────────────

def verify() -> None:
    if not INDEX_PATH.exists():
        print("index.csv not found.")
        return

    from pypdf import PdfReader

    with open(INDEX_PATH, newline="") as f:
        index_rows = list(csv.DictReader(f))

    errors: list[str] = []
    per_source: dict[str, set[int]] = {}

    for row in index_rows:
        split_path = OUT_DIR / row["split_filename"]
        src_key = f"{row['source_folder']}/{row['source_filename']}"
        page_num = int(row["page_number"])
        page_count = int(row["page_count_of_source"])

        # Track pages seen for each source
        if src_key not in per_source:
            per_source[src_key] = set()
        if page_num in per_source[src_key]:
            errors.append(f"DUPLICATE page in index: {src_key} page {page_num}")
        per_source[src_key].add(page_num)

        # Every split file exists and has exactly one page
        if not split_path.exists():
            errors.append(f"MISSING file: {row['split_filename']}")
            continue
        if not verify_split(split_path):
            errors.append(f"NOT one page: {row['split_filename']}")

    # Check page-count completeness
    for src_key, seen_pages in per_source.items():
        for row in index_rows:
            rk = f"{row['source_folder']}/{row['source_filename']}"
            if rk == src_key:
                expected = set(range(1, int(row["page_count_of_source"]) + 1))
                break
        missing = expected - seen_pages
        extra   = seen_pages - expected
        if missing:
            errors.append(f"MISSING pages for {src_key}: {sorted(missing)}")
        if extra:
            errors.append(f"EXTRA pages for {src_key}: {sorted(extra)}")

    total_expected = sum(int(r["page_count_of_source"]) for r in
                         {(r["source_folder"], r["source_filename"]): r
                          for r in index_rows}.values())

    print(f"\nVerification:")
    print(f"  Index rows:           {len(index_rows)}")
    print(f"  Source files:         {len(per_source)}")
    print(f"  Expected total pages: {total_expected}")
    print(f"  Errors:               {len(errors)}")
    if errors:
        for e in errors:
            print(f"  ERROR: {e}")
    else:
        print("  All checks passed: every source page appears exactly once, "
              "every split file opens and has exactly one page.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true",
                    help="Run verification and Airtable check only (no splits written)")
    args = ap.parse_args()

    if not args.verify:
        split_and_index()
    airtable_check()
    verify()


if __name__ == "__main__":
    main()
