from pathlib import Path

import pytest
from pypdf import PdfReader, PdfWriter

import pdf_bundle


def _blank_pdf(path: Path, pages: int) -> Path:
    w = PdfWriter()
    for _ in range(pages):
        w.add_blank_page(width=612, height=792)
    with open(path, "wb") as fh:
        w.write(fh)
    return path


def test_plan_groups_scanned_is_one_page_each():
    assert pdf_bundle.plan_groups({}, 3, has_text=False) == [[1], [2], [3]]


def test_plan_groups_text_starts_new_group_at_each_acord_25():
    types = {1: "acord_25", 2: "endorsement", 3: "acord_25", 4: "other", 5: "acord_25"}
    assert pdf_bundle.plan_groups(types, 5, has_text=True) == [[1, 2], [3, 4], [5]]


def test_plan_groups_text_leading_non_certificate_page_gets_its_own_group():
    types = {1: "other", 2: "acord_25", 3: "endorsement"}
    assert pdf_bundle.plan_groups(types, 3, has_text=True) == [[1], [2, 3]]


def test_plan_groups_single_page():
    assert pdf_bundle.plan_groups({1: "acord_25"}, 1, has_text=True) == [[1]]


def test_split_scanned_bundle_one_file_per_page(tmp_path):
    src = _blank_pdf(tmp_path / "COI forms.PDF", 4)
    parts = pdf_bundle.split_pdf(src, tmp_path)
    assert [p.name for p in parts] == [f"COI forms_cert0{i}.pdf" for i in range(1, 5)]
    assert all(len(PdfReader(str(p)).pages) == 1 for p in parts)
    assert src.exists()  # splitting never touches the original


def test_split_text_bundle_keeps_endorsement_with_its_certificate(tmp_path, monkeypatch):
    src = _blank_pdf(tmp_path / "bundle.pdf", 5)
    monkeypatch.setattr(pdf_bundle, "pdf_has_text_layer", lambda p: True)
    types = {1: "acord_25", 2: "endorsement", 3: "acord_25", 4: "acord_25", 5: "other"}
    parts = pdf_bundle.split_pdf(src, tmp_path, classify=lambda p: types)
    assert [len(PdfReader(str(p)).pages) for p in parts] == [2, 1, 2]


def test_single_page_pdf_is_not_split(tmp_path):
    src = _blank_pdf(tmp_path / "one.pdf", 1)
    assert pdf_bundle.split_pdf(src, tmp_path) == []


def test_text_pdf_with_one_certificate_is_not_split(tmp_path, monkeypatch):
    src = _blank_pdf(tmp_path / "two.pdf", 2)
    monkeypatch.setattr(pdf_bundle, "pdf_has_text_layer", lambda p: True)
    assert pdf_bundle.split_pdf(src, tmp_path, classify=lambda p: {1: "acord_25", 2: "endorsement"}) == []


def test_oversized_bundle_is_left_alone(tmp_path, monkeypatch):
    src = _blank_pdf(tmp_path / "huge.pdf", 5)
    monkeypatch.setattr(pdf_bundle, "MAX_BUNDLE_PAGES", 3)
    assert pdf_bundle.split_pdf(src, tmp_path) == []


def test_archive_original_moves_file_and_avoids_collision(tmp_path):
    a = _blank_pdf(tmp_path / "x.pdf", 2)
    first = pdf_bundle.archive_original(a, tmp_path)
    b = _blank_pdf(tmp_path / "x.pdf", 2)
    second = pdf_bundle.archive_original(b, tmp_path)
    assert first.parent.name == pdf_bundle.ORIGINALS_DIRNAME
    assert first != second and first.exists() and second.exists()
    assert not a.exists()


def test_blank_pages_have_no_text_layer(tmp_path):
    assert pdf_bundle.pdf_has_text_layer(_blank_pdf(tmp_path / "b.pdf", 2)) is False


# ── certificate_files ─────────────────────────────────────────────────────────

def test_certificate_files_single_page_returns_original(tmp_path):
    """Single-page PDF must return [pdf_path], not []."""
    src = _blank_pdf(tmp_path / "cert.pdf", 1)
    result = pdf_bundle.certificate_files(src, tmp_path)
    assert result == [src]


def test_certificate_files_single_page_does_not_split(tmp_path):
    """No split files should be written for a single-page PDF."""
    src = _blank_pdf(tmp_path / "cert.pdf", 1)
    before = set(tmp_path.iterdir())
    pdf_bundle.certificate_files(src, tmp_path)
    after = set(tmp_path.iterdir())
    assert after == before  # nothing new created


def test_certificate_files_multi_page_bundle_returns_parts(tmp_path):
    """Multi-page scanned bundle should be split and return the parts."""
    src = _blank_pdf(tmp_path / "bundle.pdf", 3)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    result = pdf_bundle.certificate_files(src, out_dir)
    assert len(result) == 3
    assert all(p.exists() for p in result)
    assert all(p != src for p in result)


def test_certificate_files_oversized_returns_empty(tmp_path, monkeypatch):
    """Files exceeding MAX_BUNDLE_PAGES should return [] (manual review needed)."""
    src = _blank_pdf(tmp_path / "huge.pdf", 5)
    monkeypatch.setattr(pdf_bundle, "MAX_BUNDLE_PAGES", 3)
    assert pdf_bundle.certificate_files(src, tmp_path) == []


def test_certificate_files_unsplittable_multi_page_returns_original(tmp_path, monkeypatch):
    """A 2-page PDF classified as one cert (no split) should return [pdf_path]."""
    src = _blank_pdf(tmp_path / "two.pdf", 2)
    monkeypatch.setattr(pdf_bundle, "pdf_has_text_layer", lambda p: True)
    # Classify both pages as endorsement (no acord_25 → split_pdf returns [])
    result = pdf_bundle.certificate_files(
        src, tmp_path, classify=lambda p: {1: "endorsement", 2: "endorsement"}
    )
    assert result == [src]


def test_certificate_files_zero_page_returns_empty(tmp_path, monkeypatch):
    """A PDF that reports 0 pages returns []."""
    from unittest.mock import MagicMock, patch
    src = tmp_path / "empty.pdf"
    src.write_bytes(b"%PDF-1.4")
    mock_reader = MagicMock()
    mock_reader.pages = []
    with patch("pypdf.PdfReader", return_value=mock_reader):
        result = pdf_bundle.certificate_files(src, tmp_path)
    assert result == []
