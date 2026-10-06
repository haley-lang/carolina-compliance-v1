"""Tests for intake fixes: collision-proof upload names and Message-ID dedup."""
from unittest.mock import MagicMock

import airtable_client
from email_monitor import _archive_message, _unique_upload_path


# ── _unique_upload_path ──────────────────────────────────────────────────────

def test_unique_path_unchanged_when_free(tmp_path):
    up, ex = tmp_path / "uploads", tmp_path / "extracted"
    up.mkdir(); ex.mkdir()
    assert _unique_upload_path(up, "COI.pdf", ex) == up / "COI.pdf"


def test_unique_path_avoids_previously_extracted_name(tmp_path):
    """A new 'COI.pdf' must not reuse a stem that already has extracted/COI.json."""
    up, ex = tmp_path / "uploads", tmp_path / "extracted"
    up.mkdir(); ex.mkdir()
    (ex / "COI.json").write_text("{}")
    assert _unique_upload_path(up, "COI.pdf", ex) == up / "COI_1.pdf"


def test_unique_path_counter_does_not_compound(tmp_path):
    """x, x_1, x_2 taken -> x_3 (not x_1_1 or x_2_1)."""
    up, ex = tmp_path / "uploads", tmp_path / "extracted"
    up.mkdir(); ex.mkdir()
    (up / "COI.pdf").write_bytes(b"x")
    (ex / "COI_1.json").write_text("{}")
    (up / "COI_2.pdf").write_bytes(b"x")
    assert _unique_upload_path(up, "COI.pdf", ex) == up / "COI_3.pdf"


def test_unique_path_handles_uppercase_extension(tmp_path):
    up, ex = tmp_path / "uploads", tmp_path / "extracted"
    up.mkdir(); ex.mkdir()
    (ex / "coi.json").write_text("{}")
    assert _unique_upload_path(up, "coi.PDF", ex) == up / "coi_1.PDF"


# ── find_document_by_message_id ──────────────────────────────────────────────

def test_find_by_message_id_empty_returns_none(monkeypatch):
    monkeypatch.setattr(airtable_client, "get_table", lambda: (_ for _ in ()).throw(AssertionError("no lookup expected")))
    assert airtable_client.find_document_by_message_id("") is None


def test_find_by_message_id_builds_escaped_formula(monkeypatch):
    table = MagicMock()
    table.first.return_value = {"id": "recX"}
    monkeypatch.setattr(airtable_client, "get_table", lambda: table)
    out = airtable_client.find_document_by_message_id("<a'b@mail.gmail.com>")
    assert out == {"id": "recX"}
    formula = table.first.call_args.kwargs["formula"]
    assert formula == "{Source Email Message ID}='<a\\'b@mail.gmail.com>'"


# ── _archive_message ─────────────────────────────────────────────────────────

def test_archive_message_marks_seen_and_moves():
    server = MagicMock()
    _archive_message(server, 42)
    server.add_flags.assert_called_once_with(42, [b"\\Seen"])
    server.copy.assert_called_once_with([42], "Processed")
    server.delete_messages.assert_called_once_with([42])
    server.expunge.assert_called_once()


def test_archive_message_survives_imap_errors():
    server = MagicMock()
    server.add_flags.side_effect = RuntimeError("boom")
    server.copy.side_effect = RuntimeError("no Processed label")
    _archive_message(server, 7)  # must not raise
