"""Tests for import_to_front.py — guards, dedup, dry run, apply, ledger."""
import csv
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("FRONT_API_TOKEN", "fake-token-for-tests")

import import_to_front as front


# ── Fixtures ──────────────────────────────────────────────────────────────────

FAKE_SHA_1 = "a" * 64
FAKE_SHA_2 = "b" * 64
FAKE_SHA_DUP = "a" * 64  # same as 1 → duplicate


def _make_index(tmp_path: Path, rows: list[dict]) -> Path:
    """Write a minimal index.csv to a temp directory."""
    idx = tmp_path / "index.csv"
    fieldnames = ["split_filename", "source_folder", "source_filename",
                  "page_number", "page_count_of_source", "sha256",
                  "has_text_layer", "is_non_cert"]
    with open(idx, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return idx


def _make_pdf(tmp_path: Path, name: str) -> Path:
    """Write a 1-byte stub PDF so file-existence checks pass."""
    p = tmp_path / name
    p.write_bytes(b"%PDF-1.4")
    return p


# ── Safety guard tests ────────────────────────────────────────────────────────

def test_require_inbox_correct_passes():
    front._require_inbox("inb_k4xex")  # must not raise


def test_require_inbox_wrong_raises():
    with pytest.raises(ValueError, match="Refused"):
        front._require_inbox("inb_other")


def test_require_inbox_empty_raises():
    with pytest.raises(ValueError, match="Refused"):
        front._require_inbox("")


def test_validate_addresses_valid():
    front._validate_addresses(
        "coi-intake@carolinacompliancesolutions.com",
        ["compliance@carolinacompliancesolutions.com"],
    )


def test_validate_addresses_bad_sender_raises():
    with pytest.raises(ValueError, match="sender.handle"):
        front._validate_addresses(
            "vendor@example.com",
            ["compliance@carolinacompliancesolutions.com"],
        )


def test_validate_addresses_bad_to_raises():
    with pytest.raises(ValueError, match="to address"):
        front._validate_addresses(
            "coi-intake@carolinacompliancesolutions.com",
            ["vendor@example.com"],
        )


def test_validate_addresses_multiple_to_all_must_pass():
    with pytest.raises(ValueError, match="to address"):
        front._validate_addresses(
            "coi-intake@carolinacompliancesolutions.com",
            [
                "compliance@carolinacompliancesolutions.com",
                "vendor@example.com",  # bad
            ],
        )


# ── Deduplication + non-cert exclusion ───────────────────────────────────────

def test_load_eligible_excludes_non_cert(tmp_path):
    _make_index(tmp_path, [
        {"split_filename": "cert_p01.pdf", "sha256": FAKE_SHA_1,
         "is_non_cert": "true", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "1", "page_count_of_source": "1", "has_text_layer": "false"},
    ])
    rows = front.load_eligible_rows(tmp_path / "index.csv")
    assert rows == []


def test_load_eligible_deduplicates_by_sha256(tmp_path):
    _make_index(tmp_path, [
        {"split_filename": "cert_p01.pdf", "sha256": FAKE_SHA_1,
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "1", "page_count_of_source": "2", "has_text_layer": "false"},
        {"split_filename": "resend_p01.pdf", "sha256": FAKE_SHA_DUP,  # same sha256
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "1", "page_count_of_source": "1", "has_text_layer": "false"},
        {"split_filename": "cert_p02.pdf", "sha256": FAKE_SHA_2,
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "2", "page_count_of_source": "2", "has_text_layer": "false"},
    ])
    rows = front.load_eligible_rows(tmp_path / "index.csv")
    names = [r["split_filename"] for r in rows]
    assert "cert_p01.pdf" in names
    assert "cert_p02.pdf" in names
    assert "resend_p01.pdf" not in names  # duplicate sha256 dropped


def test_load_eligible_keeps_cert_rows(tmp_path):
    _make_index(tmp_path, [
        {"split_filename": "cert_p01.pdf", "sha256": FAKE_SHA_1,
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "1", "page_count_of_source": "1", "has_text_layer": "false"},
    ])
    rows = front.load_eligible_rows(tmp_path / "index.csv")
    assert len(rows) == 1
    assert rows[0]["split_filename"] == "cert_p01.pdf"


# ── Message construction ──────────────────────────────────────────────────────

def test_build_message_fields(tmp_path):
    row = {
        "split_filename": "COI_forms_p01.pdf",
        "source_filename": "COI forms.PDF",
        "page_number": "1",
        "page_count_of_source": "47",
        "source_folder": "Original Certificates",
        "sha256": FAKE_SHA_1,
    }
    with patch.object(front, "SPLIT_DIR", tmp_path):
        msg = front.build_message(row)

    assert msg["inbox_id"] == "inb_k4xex"
    assert msg["subject"] == "Certificate - COI_forms_p01"
    assert msg["sender_handle"].endswith("@carolinacompliancesolutions.com")
    assert all(a.endswith("@carolinacompliancesolutions.com") for a in msg["to"])
    assert msg["external_id"] == f"ccs-rtr-{FAKE_SHA_1}"
    assert msg["tags"] == ["RTR"]
    assert msg["metadata"]["is_inbound"] is True
    # Body must not contain vendor name
    assert "RTR" not in msg["body"]
    assert "COI forms.PDF" in msg["body"]
    assert "1" in msg["body"]


# ── import_message guard tests ────────────────────────────────────────────────

def test_import_message_refuses_wrong_inbox(tmp_path):
    _make_pdf(tmp_path, "cert.pdf")
    msg = {
        "inbox_id":      "inb_WRONG",
        "sender_handle": "coi-intake@carolinacompliancesolutions.com",
        "to":            ["compliance@carolinacompliancesolutions.com"],
        "type": "email", "subject": "X", "body": "X",
        "external_id": "ext1", "created_at": 0, "tags": ["RTR"],
        "metadata": {"is_inbound": True},
        "pdf_path": tmp_path / "cert.pdf",
    }
    with pytest.raises(ValueError, match="Refused"):
        front.import_message(msg, "token")


def test_import_message_refuses_bad_sender(tmp_path):
    _make_pdf(tmp_path, "cert.pdf")
    msg = {
        "inbox_id":      "inb_k4xex",
        "sender_handle": "vendor@example.com",
        "to":            ["compliance@carolinacompliancesolutions.com"],
        "type": "email", "subject": "X", "body": "X",
        "external_id": "ext1", "created_at": 0, "tags": ["RTR"],
        "metadata": {"is_inbound": True},
        "pdf_path": tmp_path / "cert.pdf",
    }
    with pytest.raises(ValueError, match="sender.handle"):
        front.import_message(msg, "token")


# ── Dry run (no HTTP) ─────────────────────────────────────────────────────────

def test_dry_run_makes_no_http_calls(tmp_path):
    idx = _make_index(tmp_path, [
        {"split_filename": "cert_p01.pdf", "sha256": FAKE_SHA_1,
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "1", "page_count_of_source": "1", "has_text_layer": "false"},
    ])
    _make_pdf(tmp_path, "cert_p01.pdf")
    ledger_path = tmp_path / "ledger.json"

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message") as mock_import:
        results = front.run(
            apply=False,
            limit=None,
            token="fake",
            index_csv=idx,
            ledger_path=ledger_path,
        )

    mock_import.assert_not_called()
    assert len(results) == 1
    assert results[0]["status"] == "dry_run"
    assert not ledger_path.exists()


# ── Apply (mocked HTTP) ───────────────────────────────────────────────────────

def test_apply_calls_import_and_saves_ledger(tmp_path):
    idx = _make_index(tmp_path, [
        {"split_filename": "cert_p01.pdf", "sha256": FAKE_SHA_1,
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "1", "page_count_of_source": "1", "has_text_layer": "false"},
    ])
    _make_pdf(tmp_path, "cert_p01.pdf")
    ledger_path = tmp_path / "ledger.json"
    fake_response = {"status": "accepted", "message_uid": "uid123",
                     "conversation_reference": "ref456"}

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message", return_value=fake_response):
        results = front.run(
            apply=True,
            limit=None,
            token="fake",
            index_csv=idx,
            ledger_path=ledger_path,
        )

    assert len(results) == 1
    assert results[0]["status"] == "imported"
    assert results[0]["front_response"] == fake_response

    ledger = json.loads(ledger_path.read_text())
    ext_id = f"ccs-rtr-{FAKE_SHA_1}"
    assert ext_id in ledger
    assert ledger[ext_id]["message_uid"] == "uid123"


# ── Ledger deduplication across runs ─────────────────────────────────────────

def test_second_run_skips_already_imported(tmp_path):
    idx = _make_index(tmp_path, [
        {"split_filename": "cert_p01.pdf", "sha256": FAKE_SHA_1,
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "1", "page_count_of_source": "1", "has_text_layer": "false"},
    ])
    _make_pdf(tmp_path, "cert_p01.pdf")
    ledger_path = tmp_path / "ledger.json"
    ext_id = f"ccs-rtr-{FAKE_SHA_1}"
    ledger_path.write_text(json.dumps({ext_id: {"status": "accepted", "message_uid": "uid-prior"}}))

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message") as mock_import:
        results = front.run(
            apply=True,
            limit=None,
            token="fake",
            index_csv=idx,
            ledger_path=ledger_path,
        )

    mock_import.assert_not_called()
    assert results[0]["status"] == "ledger_skip"


# ── --files filter ────────────────────────────────────────────────────────────

def test_files_filter_restricts_to_named_files(tmp_path):
    idx = _make_index(tmp_path, [
        {"split_filename": "cert_p01.pdf", "sha256": FAKE_SHA_1,
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "1", "page_count_of_source": "2", "has_text_layer": "false"},
        {"split_filename": "cert_p02.pdf", "sha256": FAKE_SHA_2,
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": "2", "page_count_of_source": "2", "has_text_layer": "false"},
    ])
    _make_pdf(tmp_path, "cert_p01.pdf")
    _make_pdf(tmp_path, "cert_p02.pdf")
    ledger_path = tmp_path / "ledger.json"

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message") as mock_import:
        results = front.run(
            apply=False,
            limit=None,
            token="fake",
            index_csv=idx,
            ledger_path=ledger_path,
            files_filter=["cert_p01.pdf"],
        )

    names = [r["split_filename"] for r in results]
    assert names == ["cert_p01.pdf"]
    assert "cert_p02.pdf" not in names


# ── --limit ───────────────────────────────────────────────────────────────────

def test_limit_caps_rows_processed(tmp_path):
    rows = [
        {"split_filename": f"cert_p0{i}.pdf", "sha256": chr(ord('a') + i) * 64,
         "is_non_cert": "false", "source_folder": "X", "source_filename": "f.pdf",
         "page_number": str(i), "page_count_of_source": "5", "has_text_layer": "false"}
        for i in range(1, 6)
    ]
    idx = _make_index(tmp_path, rows)
    for r in rows:
        _make_pdf(tmp_path, r["split_filename"])
    ledger_path = tmp_path / "ledger.json"

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message"):
        results = front.run(
            apply=False, limit=2, token="fake",
            index_csv=idx, ledger_path=ledger_path,
        )

    assert len(results) == 2


# ── run() refuses wrong inbox before doing anything ───────────────────────────

def test_run_refuses_wrong_inbox(tmp_path):
    idx = _make_index(tmp_path, [])
    with pytest.raises(ValueError, match="Refused"):
        front.run(
            apply=False, limit=None, token="fake",
            index_csv=idx, ledger_path=tmp_path / "ledger.json",
            inbox_id="inb_WRONG",
        )
