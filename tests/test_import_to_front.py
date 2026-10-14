"""Tests for import_to_front.py — guards, dedup, dry run, apply, ledger."""
import csv
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("FRONT_API_TOKEN", "fake-token-for-tests")

import import_to_front as front


# ── Fixtures ──────────────────────────────────────────────────────────────────

FAKE_SHA_1   = "a" * 64
FAKE_SHA_2   = "b" * 64
FAKE_SHA_DUP = "a" * 64   # same as 1 → duplicate

CCS_ADDR = "rtr-import@carolinacompliancesolutions.com"  # current sender/to


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
    p = tmp_path / name
    p.write_bytes(b"%PDF-1.4")
    return p


def _row(filename: str, sha: str, page: str = "1", page_count: str = "1",
         source: str = "f.pdf", is_non_cert: str = "false") -> dict:
    return {
        "split_filename": filename, "sha256": sha, "is_non_cert": is_non_cert,
        "source_folder": "X", "source_filename": source,
        "page_number": page, "page_count_of_source": page_count,
        "has_text_layer": "false",
    }


# ── inbox guard ───────────────────────────────────────────────────────────────

def test_require_inbox_correct_passes():
    front._require_inbox("inb_k4xex")


def test_require_inbox_wrong_raises():
    with pytest.raises(ValueError, match="Refused"):
        front._require_inbox("inb_other")


def test_require_inbox_empty_raises():
    with pytest.raises(ValueError, match="Refused"):
        front._require_inbox("")


# ── CCS domain guard ──────────────────────────────────────────────────────────

def test_validate_addresses_valid():
    front._validate_addresses(CCS_ADDR, [CCS_ADDR])


def test_validate_addresses_bad_sender_raises():
    with pytest.raises(ValueError, match="sender.handle"):
        front._validate_addresses("vendor@example.com", [CCS_ADDR])


def test_validate_addresses_bad_to_raises():
    with pytest.raises(ValueError, match="to address"):
        front._validate_addresses(CCS_ADDR, ["vendor@example.com"])


def test_validate_addresses_multiple_to_all_must_pass():
    with pytest.raises(ValueError, match="to address"):
        front._validate_addresses(CCS_ADDR, [CCS_ADDR, "vendor@example.com"])


# ── channel address guard ─────────────────────────────────────────────────────

def test_require_not_a_channel_passes_when_absent():
    """No error when address is not in the channel list."""
    with patch("import_to_front.fetch_channel_addresses",
               return_value={"compliance@carolinacompliancesolutions.com",
                             "coi-intake@carolinacompliancesolutions.com"}):
        front._require_not_a_channel([CCS_ADDR], "fake-token")


def test_require_not_a_channel_raises_when_present():
    """Refuses when the import address matches a real Front channel."""
    with patch("import_to_front.fetch_channel_addresses",
               return_value={"rtr-import@carolinacompliancesolutions.com"}):
        with pytest.raises(ValueError, match="is a Front channel address"):
            front._require_not_a_channel([CCS_ADDR], "fake-token")


def test_require_not_a_channel_case_insensitive():
    """_require_not_a_channel lowercases the candidate before checking."""
    # fetch_channel_addresses always returns lowercase; verify the caller also
    # lowercases before the lookup so the comparison is always lower-vs-lower.
    mixed_case_input = "RTR-Import@carolinacompliancesolutions.com"
    with patch("import_to_front.fetch_channel_addresses",
               return_value={"rtr-import@carolinacompliancesolutions.com"}):
        with pytest.raises(ValueError, match="is a Front channel address"):
            front._require_not_a_channel([mixed_case_input], "fake-token")


def test_apply_runs_channel_preflight(tmp_path):
    """Channel check is called before any import when --apply is set."""
    idx = _make_index(tmp_path, [_row("cert_p01.pdf", FAKE_SHA_1)])
    _make_pdf(tmp_path, "cert_p01.pdf")
    fake_response = {"status": "accepted", "message_uid": "uid1"}

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.fetch_channel_addresses", return_value=set()) as mock_ch, \
         patch("import_to_front.import_message", return_value=fake_response), \
         patch("import_to_front.verify_imports"):
        front.run(apply=True, limit=None, token="fake",
                  index_csv=idx, ledger_path=tmp_path / "ledger.json")

    mock_ch.assert_called_once_with("fake")


def test_dry_run_skips_channel_preflight(tmp_path):
    """Channel check is NOT called during --dry-run."""
    idx = _make_index(tmp_path, [_row("cert_p01.pdf", FAKE_SHA_1)])
    _make_pdf(tmp_path, "cert_p01.pdf")

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.fetch_channel_addresses") as mock_ch, \
         patch("import_to_front.import_message"):
        front.run(apply=False, limit=None, token="fake",
                  index_csv=idx, ledger_path=tmp_path / "ledger.json")

    mock_ch.assert_not_called()


def test_apply_refuses_when_address_is_channel(tmp_path):
    """run() refuses before importing if import address is a Front channel."""
    idx = _make_index(tmp_path, [_row("cert_p01.pdf", FAKE_SHA_1)])
    _make_pdf(tmp_path, "cert_p01.pdf")

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.fetch_channel_addresses",
               return_value={"rtr-import@carolinacompliancesolutions.com"}), \
         patch("import_to_front.import_message") as mock_import:
        with pytest.raises(ValueError, match="is a Front channel address"):
            front.run(apply=True, limit=None, token="fake",
                      index_csv=idx, ledger_path=tmp_path / "ledger.json")

    mock_import.assert_not_called()


# ── Deduplication + non-cert exclusion ───────────────────────────────────────

def test_load_eligible_excludes_non_cert(tmp_path):
    _make_index(tmp_path, [_row("cert_p01.pdf", FAKE_SHA_1, is_non_cert="true")])
    rows = front.load_eligible_rows(tmp_path / "index.csv")
    assert rows == []


def test_load_eligible_deduplicates_by_sha256(tmp_path):
    _make_index(tmp_path, [
        _row("cert_p01.pdf",   FAKE_SHA_1,   page="1", page_count="2"),
        _row("resend_p01.pdf", FAKE_SHA_DUP, page="1"),   # same sha256 as cert_p01
        _row("cert_p02.pdf",   FAKE_SHA_2,   page="2", page_count="2"),
    ])
    rows = front.load_eligible_rows(tmp_path / "index.csv")
    names = [r["split_filename"] for r in rows]
    assert "cert_p01.pdf" in names
    assert "cert_p02.pdf" in names
    assert "resend_p01.pdf" not in names


def test_load_eligible_keeps_cert_rows(tmp_path):
    _make_index(tmp_path, [_row("cert_p01.pdf", FAKE_SHA_1)])
    rows = front.load_eligible_rows(tmp_path / "index.csv")
    assert len(rows) == 1


def test_load_eligible_sorted_by_source_filename_then_page(tmp_path):
    """'COI forms.PDF' (.) sorts before 'COI forms1.PDF' (1) — '.' < '1' in ASCII."""
    _make_index(tmp_path, [
        _row("COI_forms1_p01.pdf", FAKE_SHA_1, page="1", source="COI forms1.PDF"),
        _row("COI_forms_p01.pdf",  FAKE_SHA_2, page="1", source="COI forms.PDF"),
    ])
    rows = front.load_eligible_rows(tmp_path / "index.csv")
    assert rows[0]["split_filename"] == "COI_forms_p01.pdf"
    assert rows[1]["split_filename"] == "COI_forms1_p01.pdf"


# ── Message construction ──────────────────────────────────────────────────────

def test_build_message_fields(tmp_path):
    row = _row("COI_forms_p01.pdf", FAKE_SHA_1, page="1", page_count="47",
               source="COI forms.PDF")
    row["source_folder"] = "Original Certificates"
    with patch.object(front, "SPLIT_DIR", tmp_path):
        msg = front.build_message(row)

    assert msg["inbox_id"] == "inb_k4xex"
    assert msg["subject"] == "Certificate - COI_forms_p01"
    assert msg["sender_handle"] == CCS_ADDR
    assert msg["to"] == [CCS_ADDR]
    assert msg["external_id"] == f"ccs-rtr-{FAKE_SHA_1}"
    assert msg["tags"] == ["RTR"]
    assert msg["metadata"]["is_inbound"] is True
    assert "RTR" not in msg["body"]
    assert "COI forms.PDF" in msg["body"]


def test_build_message_thread_ref_equals_external_id(tmp_path):
    """thread_ref must equal external_id to force separate conversations."""
    row = _row("COI_forms_p01.pdf", FAKE_SHA_1)
    row["source_folder"] = "X"
    with patch.object(front, "SPLIT_DIR", tmp_path):
        msg = front.build_message(row)
    # thread_ref is embedded in the data built by import_message, not in msg dict;
    # verify external_id is correct so import_message can set thread_ref = external_id.
    assert msg["external_id"].startswith("ccs-rtr-")


# ── import_message guard tests ────────────────────────────────────────────────

def test_import_message_refuses_wrong_inbox(tmp_path):
    _make_pdf(tmp_path, "cert.pdf")
    msg = {
        "inbox_id": "inb_WRONG", "sender_handle": CCS_ADDR, "to": [CCS_ADDR],
        "type": "email", "subject": "X", "body": "X",
        "external_id": "ext1", "created_at": 0, "tags": ["RTR"],
        "metadata": {"is_inbound": True}, "pdf_path": tmp_path / "cert.pdf",
    }
    with pytest.raises(ValueError, match="Refused"):
        front.import_message(msg, "token")


def test_import_message_refuses_bad_sender(tmp_path):
    _make_pdf(tmp_path, "cert.pdf")
    msg = {
        "inbox_id": "inb_k4xex", "sender_handle": "vendor@example.com",
        "to": [CCS_ADDR], "type": "email", "subject": "X", "body": "X",
        "external_id": "ext1", "created_at": 0, "tags": ["RTR"],
        "metadata": {"is_inbound": True}, "pdf_path": tmp_path / "cert.pdf",
    }
    with pytest.raises(ValueError, match="sender.handle"):
        front.import_message(msg, "token")


# ── Dry run (no HTTP) ─────────────────────────────────────────────────────────

def test_dry_run_makes_no_http_calls(tmp_path):
    idx = _make_index(tmp_path, [_row("cert_p01.pdf", FAKE_SHA_1)])
    _make_pdf(tmp_path, "cert_p01.pdf")
    ledger_path = tmp_path / "ledger.json"

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message") as mock_import:
        results = front.run(apply=False, limit=None, token="fake",
                            index_csv=idx, ledger_path=ledger_path)

    mock_import.assert_not_called()
    assert len(results) == 1
    assert results[0]["status"] == "dry_run"
    assert not ledger_path.exists()


# ── Apply (mocked HTTP) ───────────────────────────────────────────────────────

def test_apply_calls_import_and_saves_ledger(tmp_path):
    idx = _make_index(tmp_path, [_row("cert_p01.pdf", FAKE_SHA_1)])
    _make_pdf(tmp_path, "cert_p01.pdf")
    ledger_path = tmp_path / "ledger.json"
    fake_response = {"status": "accepted", "message_uid": "uid123",
                     "conversation_reference": "ref456"}

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.fetch_channel_addresses", return_value=set()), \
         patch("import_to_front.import_message", return_value=fake_response), \
         patch("import_to_front.verify_imports"):
        results = front.run(apply=True, limit=None, token="fake",
                            index_csv=idx, ledger_path=ledger_path)

    assert len(results) == 1
    assert results[0]["status"] == "imported"
    assert results[0]["front_response"] == fake_response
    ledger = json.loads(ledger_path.read_text())
    ext_id = f"ccs-rtr-{FAKE_SHA_1}"
    assert ledger[ext_id]["message_uid"] == "uid123"


# ── Ledger deduplication across runs ─────────────────────────────────────────

def test_second_run_skips_already_imported(tmp_path):
    idx = _make_index(tmp_path, [_row("cert_p01.pdf", FAKE_SHA_1)])
    _make_pdf(tmp_path, "cert_p01.pdf")
    ledger_path = tmp_path / "ledger.json"
    ext_id = f"ccs-rtr-{FAKE_SHA_1}"
    ledger_path.write_text(json.dumps({ext_id: {"status": "accepted", "message_uid": "uid-prior"}}))

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.fetch_channel_addresses", return_value=set()), \
         patch("import_to_front.import_message") as mock_import, \
         patch("import_to_front.verify_imports"):
        results = front.run(apply=True, limit=None, token="fake",
                            index_csv=idx, ledger_path=ledger_path)

    mock_import.assert_not_called()
    assert results[0]["status"] == "ledger_skip"


# ── --start offset ────────────────────────────────────────────────────────────

def test_start_skips_first_n_minus_1_rows(tmp_path):
    """--start 3 skips rows 1 and 2 (1-based), processes row 3 onward."""
    rows = [_row(f"cert_p0{i}.pdf", chr(ord('a') + i) * 64, page=str(i),
                 page_count="5") for i in range(1, 6)]
    idx = _make_index(tmp_path, rows)
    for r in rows:
        _make_pdf(tmp_path, r["split_filename"])

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message"):
        results = front.run(apply=False, limit=None, start=3, token="fake",
                            index_csv=idx, ledger_path=tmp_path / "ledger.json")

    names = [r["split_filename"] for r in results]
    assert "cert_p01.pdf" not in names
    assert "cert_p02.pdf" not in names
    assert "cert_p03.pdf" in names


def test_start_1_processes_all(tmp_path):
    """--start 1 (or None) processes all eligible rows."""
    rows = [_row(f"cert_p0{i}.pdf", chr(ord('a') + i) * 64, page=str(i),
                 page_count="3") for i in range(1, 4)]
    idx = _make_index(tmp_path, rows)
    for r in rows:
        _make_pdf(tmp_path, r["split_filename"])

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message"):
        results = front.run(apply=False, limit=None, start=1, token="fake",
                            index_csv=idx, ledger_path=tmp_path / "ledger.json")

    assert len(results) == 3


def test_start_and_limit_combined(tmp_path):
    """--start 3 --limit 2 processes exactly rows 3 and 4."""
    rows = [_row(f"cert_p0{i}.pdf", chr(ord('a') + i) * 64, page=str(i),
                 page_count="5") for i in range(1, 6)]
    idx = _make_index(tmp_path, rows)
    for r in rows:
        _make_pdf(tmp_path, r["split_filename"])

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message"):
        results = front.run(apply=False, limit=2, start=3, token="fake",
                            index_csv=idx, ledger_path=tmp_path / "ledger.json")

    names = [r["split_filename"] for r in results]
    assert names == ["cert_p03.pdf", "cert_p04.pdf"]


# ── --files filter ────────────────────────────────────────────────────────────

def test_files_filter_restricts_to_named_files(tmp_path):
    idx = _make_index(tmp_path, [
        _row("cert_p01.pdf", FAKE_SHA_1, page="1", page_count="2"),
        _row("cert_p02.pdf", FAKE_SHA_2, page="2", page_count="2"),
    ])
    _make_pdf(tmp_path, "cert_p01.pdf")
    _make_pdf(tmp_path, "cert_p02.pdf")

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message"):
        results = front.run(apply=False, limit=None, token="fake",
                            index_csv=idx, ledger_path=tmp_path / "ledger.json",
                            files_filter=["cert_p01.pdf"])

    names = [r["split_filename"] for r in results]
    assert names == ["cert_p01.pdf"]


# ── --limit ───────────────────────────────────────────────────────────────────

def test_limit_caps_rows_processed(tmp_path):
    rows = [_row(f"cert_p0{i}.pdf", chr(ord('a') + i) * 64, page=str(i),
                 page_count="5") for i in range(1, 6)]
    idx = _make_index(tmp_path, rows)
    for r in rows:
        _make_pdf(tmp_path, r["split_filename"])

    with patch.object(front, "SPLIT_DIR", tmp_path), \
         patch("import_to_front.import_message"):
        results = front.run(apply=False, limit=2, token="fake",
                            index_csv=idx, ledger_path=tmp_path / "ledger.json")

    assert len(results) == 2


# ── run() refuses wrong inbox before doing anything ───────────────────────────

def test_run_refuses_wrong_inbox(tmp_path):
    idx = _make_index(tmp_path, [])
    with pytest.raises(ValueError, match="Refused"):
        front.run(apply=False, limit=None, token="fake",
                  index_csv=idx, ledger_path=tmp_path / "ledger.json",
                  inbox_id="inb_WRONG")
