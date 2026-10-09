"""Split combined certificate PDFs into one file per certificate.

Clients often send one large PDF containing many certificates (for example 47
scanned ACORD 25 pages). The extractor reads one certificate per file, so a
bundle is split before extraction.

Rules:
- A PDF with a text layer is grouped by page type: each ACORD 25 page starts a
  new certificate and any following non-ACORD-25 pages (endorsements, notes)
  stay attached to it.
- A scanned PDF (no text layer on any page) cannot be classified without
  reading the images, so every page becomes its own file. The extractor's
  document_type result then separates real certificates from other pages.
- Single-page PDFs and PDFs with only one certificate are left alone.
"""

import logging
from pathlib import Path
from typing import Callable, Dict, List, Optional

log = logging.getLogger(__name__)

MIN_TEXT_CHARS = 20          # fewer characters than this = page has no text layer
ORIGINALS_DIRNAME = "_originals"
MAX_BUNDLE_PAGES = 300       # safety cap; larger files are left for manual review


def pdf_has_text_layer(pdf_path: Path) -> bool:
    """True if at least one page has a usable text layer."""
    import pdfplumber

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            if len((page.extract_text() or "").strip()) >= MIN_TEXT_CHARS:
                return True
    return False


def plan_groups(page_types: Dict[int, str], page_count: int, has_text: bool,
                acord_25: str = "acord_25") -> List[List[int]]:
    """Return a list of page-number groups (1-indexed), one group per certificate."""
    pages = list(range(1, page_count + 1))
    if page_count <= 1:
        return [pages] if pages else []
    if not has_text:
        return [[p] for p in pages]

    groups: List[List[int]] = []
    for p in pages:
        if page_types.get(p) == acord_25 or not groups:
            groups.append([p])
        else:
            groups[-1].append(p)
    return groups


def split_pdf(pdf_path: Path, out_dir: Path,
              classify: Optional[Callable[[Path], Dict[int, str]]] = None) -> List[Path]:
    """Split pdf_path into per-certificate PDFs in out_dir.

    Returns the list of new files, or [] when no split is needed. The original
    file is not touched here; the caller decides what to do with it.
    """
    from pypdf import PdfReader, PdfWriter

    reader = PdfReader(str(pdf_path))
    page_count = len(reader.pages)
    if page_count <= 1:
        return []
    if page_count > MAX_BUNDLE_PAGES:
        log.warning("%s has %d pages (over %d) — not splitting automatically",
                    pdf_path.name, page_count, MAX_BUNDLE_PAGES)
        return []

    has_text = pdf_has_text_layer(pdf_path)
    page_types = classify(pdf_path) if (classify and has_text) else {}
    groups = plan_groups(page_types, page_count, has_text)
    if len(groups) <= 1:
        return []

    out_dir.mkdir(parents=True, exist_ok=True)
    width = max(2, len(str(len(groups))))
    created: List[Path] = []
    for idx, pages in enumerate(groups, start=1):
        writer = PdfWriter()
        for p in pages:
            writer.add_page(reader.pages[p - 1])
        dest = out_dir / f"{pdf_path.stem}_cert{idx:0{width}d}.pdf"
        with open(dest, "wb") as fh:
            writer.write(fh)
        created.append(dest)
    log.info("Split %s (%d pages) into %d certificate file(s)",
             pdf_path.name, page_count, len(created))
    return created


def certificate_files(pdf_path: Path, out_dir: Path,
                      classify: Optional[Callable[[Path], Dict[int, str]]] = None
                      ) -> List[Path]:
    """Return the certificate PDFs produced from pdf_path.

    Unlike split_pdf(), this function always returns a non-empty list for
    valid PDFs:
    - Single-page PDFs are returned as-is: [pdf_path].
    - Multi-page bundles are split; the list of split files is returned.
    - Multi-page PDFs that couldn't be divided into separate groups are
      returned as-is: [pdf_path] (treated as one certificate).
    - Oversized files (> MAX_BUNDLE_PAGES) return [] — those need manual review.
    - Any file that cannot be opened returns [].

    Use this instead of split_pdf() when you need to enumerate certificate
    files from a folder, including single-page PDFs that need no splitting.
    """
    from pypdf import PdfReader

    try:
        page_count = len(PdfReader(str(pdf_path)).pages)
    except Exception as exc:
        log.warning("Could not read %s: %s", pdf_path.name, exc)
        return []

    if page_count == 0:
        return []
    if page_count > MAX_BUNDLE_PAGES:
        log.warning("%s has %d pages (> %d) — skipping; needs manual review",
                    pdf_path.name, page_count, MAX_BUNDLE_PAGES)
        return []
    if page_count == 1:
        return [pdf_path]

    parts = split_pdf(pdf_path, out_dir, classify)
    return parts if parts else [pdf_path]


def archive_original(pdf_path: Path, upload_dir: Path) -> Path:
    """Move a split bundle out of the pending folder so it is not read again."""
    dest_dir = upload_dir / ORIGINALS_DIRNAME
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / pdf_path.name
    n = 1
    while dest.exists():
        dest = dest_dir / f"{pdf_path.stem}_{n}{pdf_path.suffix}"
        n += 1
    pdf_path.rename(dest)
    return dest
