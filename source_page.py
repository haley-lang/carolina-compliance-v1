"""
Keeps the page each extraction was read from, as an Airtable attachment on the
Incoming Extractions row ("Source Page" field). Needed by the review screen and
for audits. Never raises: a failed attachment must not block intake.
"""
import logging
from pathlib import Path
from typing import Optional

import config

logger = logging.getLogger(__name__)

SOURCE_PAGE_FIELD = "Source Page"
_EXTS = (".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif")


def find_source_file(json_name: str, search_dirs=None) -> Optional[Path]:
    """Find the file an extraction came from, e.g. COI_forms_cert11.json -> uploads/COI_forms_cert11.pdf."""
    if not json_name:
        return None
    stem = Path(json_name).stem
    dirs = search_dirs or [Path(config.UPLOAD_DIR), Path(__file__).parent / "incoming_pdfs"]
    for d in dirs:
        d = Path(d)
        if not d.exists():
            continue
        for ext in _EXTS:
            for cand in (d / f"{stem}{ext}", d / f"{stem}{ext.upper()}"):
                if cand.exists():
                    return cand
    return None


def attach_source_page(table, record_id: str, json_name: str, search_dirs=None) -> bool:
    """Upload the source page file onto the row. Returns True on success, never raises."""
    try:
        path = find_source_file(json_name, search_dirs)
        if path is None:
            logger.warning("[source_page] no file found for %s — row left without attachment", json_name)
            return False
        table.upload_attachment(record_id, SOURCE_PAGE_FIELD, str(path))
        logger.info("[source_page] attached %s to %s", path.name, record_id)
        return True
    except Exception as exc:
        logger.warning("[source_page] attach failed for %s: %s", json_name, exc)
        return False
