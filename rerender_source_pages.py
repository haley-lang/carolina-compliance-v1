"""Re-render RTR/Billy cert pages at 300 DPI and add as PNG attachments.

Reads existing Source Page PDFs back from their original bundled files,
renders at 300 DPI, and uploads the PNG as an additional attachment on each
Incoming Extractions row. The existing PDF attachment is kept (never replaced).

Usage:
    # Dry run — reports row count and total PNG size, touches nothing
    .venv/bin/python rerender_source_pages.py

    # Apply — uploads PNGs to Airtable
    .venv/bin/python rerender_source_pages.py --apply
"""
import argparse
import io
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── PDF locations ─────────────────────────────────────────────────────────────

CERT_DIR  = Path("/Users/haleybridges/Desktop/RTR Construction LLC/Original Certificates")
RESEND_DIR = Path("/Users/haleybridges/Desktop/RTR Construction LLC/To Resend")

# Map source-filename prefix → (pdf_path, total_pages_in_pdf)
# Each scanned PDF was split 1-page-per-cert, so cert N == PDF page N.
PDF_MAP: dict[str, tuple[Path, int]] = {
    "COI_forms":                (CERT_DIR  / "COI forms.PDF",              47),
    "RTR_COI_forms1_resend":    (RESEND_DIR / "RTR_COI_forms1_resend.pdf", 8),
    "RTR_COI_page47_resend":    (RESEND_DIR / "RTR_COI_page47_resend.pdf", 1),
}

DPI = 300

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_source_filename(name: str) -> tuple[str, int] | None:
    """Return (pdf_prefix, 1-based page number) or None if name doesn't match."""
    # e.g. COI_forms_cert31.json  →  ("COI_forms", 31)
    m = re.match(r"^(.+)_cert(\d+)\.json$", name)
    if m:
        return m.group(1), int(m.group(2))
    # e.g. RTR_COI_page47_resend.json  →  single-page PDF, no _certN suffix
    plain = re.match(r"^(.+)\.json$", name)
    if plain:
        prefix = plain.group(1)
        if prefix in PDF_MAP:
            return prefix, 1  # single-page: always page 1
    return None


def _render_page_png(pdf_path: Path, page_num: int) -> bytes:
    """Render one PDF page (1-indexed) at DPI and return PNG bytes."""
    try:
        from pdf2image import convert_from_path
    except ImportError:
        sys.exit("pdf2image is required: pip install pdf2image")

    pages = convert_from_path(str(pdf_path), dpi=DPI, first_page=page_num, last_page=page_num)
    if not pages:
        raise ValueError(f"No pages returned for {pdf_path.name} page {page_num}")
    buf = io.BytesIO()
    pages[0].save(buf, format="PNG")
    return buf.getvalue()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Upload PNGs to Airtable (default: dry run only)")
    args = parser.parse_args()

    import config
    try:
        config.validate_config()
    except Exception as exc:
        sys.exit(f"Config error: {exc}")

    import pyairtable
    base_id = config.AIRTABLE_BASE_ID
    api_key = config.AIRTABLE_API_KEY

    api = pyairtable.Api(api_key)
    table = api.table(base_id, "tblT88Ty6d6M766oY")  # Incoming Extractions

    log.info("Fetching Incoming Extractions rows…")
    SOURCE_FILENAME_FIELD = "Source Filename"
    SOURCE_PAGE_FIELD     = "Source Page"

    all_records = table.all(fields=[SOURCE_FILENAME_FIELD, SOURCE_PAGE_FIELD])
    log.info("  %d total rows", len(all_records))

    # Identify RTR rows that need re-rendering
    target_rows: list[tuple[str, str, Path, int]] = []  # (record_id, source_fn, pdf_path, page_num)
    skip_already_has_image = 0

    for rec in all_records:
        fields = rec.get("fields", {})
        source_fn = fields.get(SOURCE_FILENAME_FIELD, "")
        parsed = _parse_source_filename(source_fn)
        if parsed is None:
            continue

        prefix, page_num = parsed
        if prefix not in PDF_MAP:
            continue

        # Skip rows with no Source Page attachment at all (bundle metadata rows like
        # COI_forms.json don't represent a single cert page and have no attachment)
        existing_atts = fields.get(SOURCE_PAGE_FIELD) or []
        if not existing_atts:
            log.debug("  SKIP (no attachment) %s", source_fn)
            continue

        # Skip rows that already have an image/* attachment (already re-rendered)
        has_image = any((a.get("type") or "").startswith("image/") for a in existing_atts)
        if has_image:
            skip_already_has_image += 1
            log.debug("  SKIP (already has image) %s", source_fn)
            continue

        pdf_path, _total = PDF_MAP[prefix]
        target_rows.append((rec["id"], source_fn, pdf_path, page_num))

    log.info(
        "%d rows need re-rendering  |  %d already have an image attachment (skipped)",
        len(target_rows), skip_already_has_image,
    )

    if not target_rows:
        log.info("Nothing to do.")
        return

    # Verify all PDFs are present before rendering anything
    missing_pdfs = set()
    for _, _, pdf_path, _ in target_rows:
        if not pdf_path.exists():
            missing_pdfs.add(pdf_path)
    if missing_pdfs:
        for p in sorted(missing_pdfs):
            log.error("  MISSING: %s", p)
        sys.exit("Aborting — one or more source PDFs not found at expected paths.")

    # Render all unique (pdf, page) pairs once; cache bytes for both sizing and upload.
    log.info("Rendering all %d pages at %d DPI…", len(target_rows), DPI)
    rendered: dict[tuple[str, int], bytes] = {}
    total_bytes = 0
    for _, source_fn, pdf_path, page_num in target_rows:
        k = (pdf_path.name, page_num)
        if k not in rendered:
            png = _render_page_png(pdf_path, page_num)
            rendered[k] = png
            log.info("  rendered %s page %d → %d KB", pdf_path.name, page_num, len(png) // 1024)
        total_bytes += len(rendered[k])

    print()
    print("=" * 60)
    print(f"  Rows to re-render:   {len(target_rows)}")
    print(f"  Total upload size:   {total_bytes / 1_048_576:.1f} MB  ({total_bytes:,} bytes)")
    print(f"  DPI:                 {DPI}")
    print("=" * 60)

    if not args.apply:
        print()
        print("DRY RUN — no changes made.")
        print("Re-run with --apply to upload the PNGs.")
        return

    # Apply: upload PNGs (bytes already in memory)
    print()
    log.info("Uploading %d PNGs…", len(target_rows))
    ok = 0
    fail = 0
    for i, (record_id, source_fn, pdf_path, page_num) in enumerate(target_rows, 1):
        png = rendered[(pdf_path.name, page_num)]
        try:
            filename = source_fn.replace(".json", "_300dpi.png")
            table.upload_attachment(record_id, SOURCE_PAGE_FIELD, filename,
                                    content=png, content_type="image/png")
            log.info("[%d/%d] ✓ %s → %s  (%d KB)",
                     i, len(target_rows), source_fn, filename, len(png) // 1024)
            ok += 1
        except Exception as exc:
            log.error("[%d/%d] FAIL %s: %s", i, len(target_rows), source_fn, exc)
            fail += 1

    print()
    print(f"Done. {ok} succeeded, {fail} failed.")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
