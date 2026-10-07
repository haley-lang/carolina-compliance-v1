"""
One-time backfill: attach the source page to existing Incoming Extractions rows.

Usage (from the project folder, with .env present):
    python backfill_source_pages.py "<folder with the original bundle PDFs>"          # dry run
    python backfill_source_pages.py "<folder with the original bundle PDFs>" --apply  # upload

Re-splits the original PDFs with the same splitter the pipeline uses, so the
per-certificate files are identical to the ones that were read. Rows are matched
by Source Filename (e.g. COI_forms_cert11.json). Rows that already have a Source
Page are skipped. Nothing is deleted or changed except adding the attachment.
"""
import re
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()
import config  # noqa: E402
import pdf_bundle  # noqa: E402
import source_page  # noqa: E402
from airtable_importer import INCOMING_EXTRACTIONS_TABLE, clean_base_id  # noqa: E402


def norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", Path(name).stem.lower()).strip("_")


def build_page_map(folder: Path, work: Path) -> dict:
    """normalized per-certificate stem -> split PDF path"""
    pages = {}
    for pdf in sorted(folder.glob("*")):
        if pdf.suffix.lower() != ".pdf":
            continue
        out = work / norm(pdf.name)
        out.mkdir(parents=True, exist_ok=True)
        for part in pdf_bundle.split_pdf(pdf, out) or []:
            pages[norm(Path(part).name)] = Path(part)
    return pages


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    folder = Path(sys.argv[1]).expanduser()
    apply = "--apply" in sys.argv
    table = Api(config.AIRTABLE_API_KEY.strip()).table(
        clean_base_id(config.AIRTABLE_BASE_ID), INCOMING_EXTRACTIONS_TABLE)
    with tempfile.TemporaryDirectory() as tmp:
        pages = build_page_map(folder, Path(tmp))
        print(f"Split originals into {len(pages)} certificate files")
        done = missing = skipped = failed = 0
        for rec in table.all():
            f = rec["fields"]
            name = f.get("Source Filename") or ""
            if f.get(source_page.SOURCE_PAGE_FIELD):
                skipped += 1
                continue
            path = pages.get(norm(name))
            if not path:
                missing += 1
                print(f"  no page found for {name}")
                continue
            if not apply:
                print(f"  would attach {path.name} -> {name}")
                done += 1
                continue
            try:
                table.upload_attachment(rec["id"], source_page.SOURCE_PAGE_FIELD, str(path))
                done += 1
                print(f"  attached {name}")
            except Exception as exc:
                failed += 1
                print(f"  FAILED {name}: {exc}")
        print(f"{'Attached' if apply else 'Would attach'}: {done}  already had: {skipped}  "
              f"no match: {missing}  failed: {failed}")
        if not apply:
            print("Dry run only. Add --apply to upload.")


if __name__ == "__main__":
    main()
