"""Tests for reextract_rtr.py"""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import reextract_rtr

SAMPLE_EXTRACTION = {
    "document_type": "COI",
    "named_insured": "RTR Construction LLC",
    "certificate_holder": "Carolina Compliance Solutions",
    "contact_emails": ["rtr@example.com"],
    "policies": [
        {
            "policy_type": "GL",
            "policy_number": "GL-001",
            "carrier": "Acme Insurance",
            "effective_date": "2025-01-01",
            "expiration_date": "2026-01-01",
            "coverage_limits": "$1,000,000",
        }
    ],
    "confidence": 0.98,
}

OLD_RAW = json.dumps({"document_type": "COI", "named_insured": "RTR (old extraction)"})


BACKUP_VALUE = '{"backed": "up"}'


def _make_rec(rec_id, source_filename, review_status="Auto-Approved",
              raw_json=None, has_backup=False):
    fields = {
        "Source Filename": source_filename,
        "Review Status": review_status,
        "Review Reason": "N/A",
        "Processing Status": "Imported",
        "Raw JSON": raw_json if raw_json is not None else OLD_RAW,
    }
    if has_backup:
        # pyairtable returns fields keyed by NAME, not field ID
        fields[reextract_rtr.PRIOR_RAW_JSON_FIELD_NAME] = BACKUP_VALUE
    return {"id": rec_id, "fields": fields}


@pytest.fixture
def cert_folder(tmp_path):
    """Folder with one fake bundle PDF."""
    folder = tmp_path / "certs"
    folder.mkdir()
    (folder / "bundle.pdf").write_bytes(b"%PDF-1.4")
    return folder


@pytest.fixture
def fake_split_page(tmp_path):
    """Fake per-certificate PDF; its stem matches Source Filename 'cert01.json'."""
    page = tmp_path / "cert01.pdf"
    page.write_bytes(b"%PDF-1.4")
    return page


@pytest.fixture(autouse=True)
def patch_split_pdf(fake_split_page):
    with patch("pdf_bundle.split_pdf", return_value=[str(fake_split_page)]):
        yield


@pytest.fixture(autouse=True)
def mock_extract():
    """Patches all extractor functions; yields the extract_document mock."""
    m = MagicMock(return_value=dict(SAMPLE_EXTRACTION))
    with patch("extractor.extract_document", m), \
         patch("extractor.normalize_policy_dates", side_effect=lambda d: d), \
         patch("extractor.apply_simple_document_classification", side_effect=lambda d, f: d), \
         patch("extractor.drop_empty_policies", side_effect=lambda d: d):
        yield m


# ── dry run ──────────────────────────────────────────────────────────────────

def test_dry_run_writes_nothing(cert_folder, mock_table_factory):
    table = mock_table_factory("tbl1")
    table._records["rec1"] = _make_rec("rec1", "cert01.json")
    table.update = MagicMock()

    reextract_rtr.run(cert_folder, apply=False, table=table)

    table.update.assert_not_called()


# ── backup logic ──────────────────────────────────────────────────────────────

def test_apply_backup_written_on_first_run(cert_folder, mock_table_factory):
    """Row has Raw JSON and no backup: backup is written with the OLD Raw JSON exactly once."""
    table = mock_table_factory("tbl1")
    table._records["rec1"] = _make_rec("rec1", "cert01.json", has_backup=False)
    table.update = MagicMock()

    reextract_rtr.run(cert_folder, apply=True, table=table)

    table.update.assert_called_once()
    fields_written = table.update.call_args[0][1]
    # backup is written using the field ID (rename-safe write)
    assert reextract_rtr.PRIOR_RAW_JSON_FIELD_ID in fields_written
    assert fields_written[reextract_rtr.PRIOR_RAW_JSON_FIELD_ID] == OLD_RAW


def test_apply_backup_not_overwritten_on_second_run(cert_folder, mock_table_factory):
    """Row already has 'Prior Raw JSON (superseded)' filled: backup value must not change."""
    table = mock_table_factory("tbl1")
    table._records["rec1"] = _make_rec("rec1", "cert01.json", has_backup=True)
    table.update = MagicMock()

    reextract_rtr.run(cert_folder, apply=True, table=table)

    # update is still called (for the other refreshed fields), but must not touch the backup
    table.update.assert_called_once()
    fields_written = table.update.call_args[0][1]
    assert reextract_rtr.PRIOR_RAW_JSON_FIELD_ID not in fields_written
    assert reextract_rtr.PRIOR_RAW_JSON_FIELD_NAME not in fields_written


# ── rejected rows ─────────────────────────────────────────────────────────────

def test_rejected_rows_skipped(cert_folder, mock_table_factory, mock_extract):
    table = mock_table_factory("tbl1")
    table._records["rec1"] = _make_rec("rec1", "cert01.json", review_status="Rejected")
    table.update = MagicMock()

    result = reextract_rtr.run(cert_folder, apply=True, table=table)

    mock_extract.assert_not_called()
    table.update.assert_not_called()
    assert result["rejected"] == 1
    assert result["matched"] == 0


# ── status fields never touched ───────────────────────────────────────────────

def test_status_fields_not_touched(cert_folder, mock_table_factory):
    table = mock_table_factory("tbl1")
    table._records["rec1"] = _make_rec("rec1", "cert01.json")
    table.update = MagicMock()

    reextract_rtr.run(cert_folder, apply=True, table=table)

    fields_written = table.update.call_args[0][1]
    assert "Review Status" not in fields_written
    assert "Review Reason" not in fields_written
    assert "Processing Status" not in fields_written
