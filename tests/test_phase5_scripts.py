"""
Tests for Phase 5 scripts:
  - report_corrections.py
  - export_gold_candidates.py

All Airtable calls are mocked; no real API calls are made.
"""

import json
import sys
from pathlib import Path
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(fields: dict) -> dict:
    return {"id": "recTest123", "fields": fields}


_SAMPLE_RECORDS = [
    _make_record({
        "Field": "named_insured",
        "Model": "claude-sonnet-4-6",
        "Learning Status": "New",
        "Source Filename": "coi_abc.json",
        "Old Value": "ABC Co",
        "New Value": "ABC Company LLC",
        "Why (Haley's note)": "Full legal name",
        "Corrected At": "2026-10-01T10:00:00Z",
        "Incoming Extraction Record ID": "recInc001",
    }),
    _make_record({
        "Field": "expiration_date",
        "Model": "claude-opus-4-5",
        "Learning Status": "Trained",
        "Source Filename": "coi_xyz.json",
        "Old Value": "2025-12-31",
        "New Value": "2026-01-01",
        "Why (Haley's note)": "OCR misread",
        "Corrected At": "2026-10-02T11:00:00Z",
        "Incoming Extraction Record ID": "recInc002",
    }),
    _make_record({
        "Field": "named_insured",
        "Model": "claude-sonnet-4-6",
        "Learning Status": "New",
        "Source Filename": "coi_abc.json",
        "Old Value": "XYZ Inc",
        "New Value": "XYZ Incorporated",
        "Why (Haley's note)": "Abbr expanded",
        "Corrected At": "2026-10-03T09:00:00Z",
        "Incoming Extraction Record ID": "recInc003",
    }),
]


# ---------------------------------------------------------------------------
# report_corrections tests
# ---------------------------------------------------------------------------

class TestReportCorrections:

    def _run_report(self, records, capsys=None):
        """Import and run report_corrections.run() with a mocked table."""
        import importlib

        # Patch pyairtable.Api so table.all() returns our records
        mock_api_instance = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = records
        mock_api_instance.table.return_value = mock_table

        with patch("report_corrections.Api", return_value=mock_api_instance):
            import report_corrections
            importlib.reload(report_corrections)  # re-run module-level code cleanly
            with patch("report_corrections.Api", return_value=mock_api_instance):
                report_corrections.run()

    def test_report_corrections_empty_table(self, capsys):
        """Empty table must run without error and print 'No corrections yet.'"""
        mock_api = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = []
        mock_api.table.return_value = mock_table

        with patch("report_corrections.Api", return_value=mock_api):
            import report_corrections
            report_corrections.run()

        captured = capsys.readouterr()
        assert "No corrections yet." in captured.out

    def test_report_corrections_with_data(self, capsys):
        """With 3 sample rows, all section headers must appear in output."""
        mock_api = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = _SAMPLE_RECORDS
        mock_api.table.return_value = mock_table

        with patch("report_corrections.Api", return_value=mock_api):
            import report_corrections
            report_corrections.run()

        captured = capsys.readouterr()
        out = captured.out

        assert "=== Corrections Report ===" in out
        assert "Total corrections: 3" in out
        assert "By field:" in out
        assert "By model:" in out
        assert "By Learning Status:" in out
        assert "By source file" in out

    def test_report_corrections_counts_are_correct(self, capsys):
        """Verify named_insured appears twice and expiration_date once."""
        mock_api = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = _SAMPLE_RECORDS
        mock_api.table.return_value = mock_table

        with patch("report_corrections.Api", return_value=mock_api):
            import report_corrections
            report_corrections.run()

        captured = capsys.readouterr()
        out = captured.out
        assert "named_insured: 2" in out
        assert "expiration_date: 1" in out

    def test_report_corrections_blank_field_handling(self, capsys):
        """Rows with missing field values must be counted as '(blank)'."""
        records = [
            _make_record({}),   # all fields missing
            _make_record({"Field": "policy_number", "Model": None, "Learning Status": "New"}),
        ]
        mock_api = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = records
        mock_api.table.return_value = mock_table

        with patch("report_corrections.Api", return_value=mock_api):
            import report_corrections
            report_corrections.run()

        captured = capsys.readouterr()
        out = captured.out
        assert "(blank)" in out


# ---------------------------------------------------------------------------
# export_gold_candidates tests
# ---------------------------------------------------------------------------

class TestExportGoldCandidates:

    @pytest.fixture(autouse=True)
    def isolate_output(self, tmp_path, monkeypatch):
        """Redirect GOLD_DIR to a temp directory so tests don't touch the real file."""
        import export_gold_candidates
        monkeypatch.setattr(export_gold_candidates, "GOLD_DIR", tmp_path)
        monkeypatch.setattr(
            export_gold_candidates, "CANDIDATES_PATH", tmp_path / "rtr_gold_candidates.json"
        )
        # Keep the forbidden path pointing elsewhere so the assert doesn't fire
        monkeypatch.setattr(
            export_gold_candidates, "_FORBIDDEN_PATH", tmp_path / "rtr_gold.json"
        )

    def test_export_gold_candidates_empty(self, tmp_path, capsys):
        """Empty table writes a candidates file with an empty list."""
        import export_gold_candidates

        mock_api = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = []
        mock_api.table.return_value = mock_table

        with patch("export_gold_candidates.Api", return_value=mock_api):
            export_gold_candidates.run()

        out_path = tmp_path / "rtr_gold_candidates.json"
        assert out_path.exists(), "Candidates file must be created even for empty table"

        payload = json.loads(out_path.read_text())
        # Empty case writes dict with "candidates" key
        assert isinstance(payload, dict)
        assert payload["candidates"] == []

        captured = capsys.readouterr()
        assert "0 candidates" in captured.out

    def test_export_gold_candidates_writes_candidates(self, tmp_path, capsys):
        """Mock 2 'New' rows → candidates file contains them."""
        import export_gold_candidates

        new_records = [r for r in _SAMPLE_RECORDS if r["fields"].get("Learning Status") == "New"]
        assert len(new_records) == 2

        mock_api = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = new_records
        mock_api.table.return_value = mock_table

        with patch("export_gold_candidates.Api", return_value=mock_api):
            export_gold_candidates.run()

        out_path = tmp_path / "rtr_gold_candidates.json"
        assert out_path.exists()
        candidates = json.loads(out_path.read_text())
        assert isinstance(candidates, list)
        assert len(candidates) == 2

        # Check structure of first candidate
        c = candidates[0]
        assert c["status"] == "candidate"
        assert c["source_page_url"] is None
        assert "field_path" in c
        assert "corrected_value" in c
        assert "old_value" in c

        captured = capsys.readouterr()
        assert "2 candidate(s)" in captured.out

    def test_export_gold_candidates_filters_non_new(self, tmp_path, capsys):
        """Rows with Learning Status != 'New' must be excluded."""
        import export_gold_candidates

        mock_api = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = _SAMPLE_RECORDS  # contains 2 New, 1 Trained
        mock_api.table.return_value = mock_table

        with patch("export_gold_candidates.Api", return_value=mock_api):
            export_gold_candidates.run()

        out_path = tmp_path / "rtr_gold_candidates.json"
        candidates = json.loads(out_path.read_text())
        assert len(candidates) == 2  # only the 2 "New" rows

    def test_export_gold_candidates_never_overwrites_gold_json(self, tmp_path):
        """The script must NEVER write to rtr_gold.json."""
        import export_gold_candidates

        # Create a real rtr_gold.json in tmp_path to detect any overwrite attempt
        gold_json_path = tmp_path / "rtr_gold.json"
        gold_json_path.write_text('{"approved": true}')
        original_content = gold_json_path.read_text()

        mock_api = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = _SAMPLE_RECORDS
        mock_api.table.return_value = mock_table

        with patch("export_gold_candidates.Api", return_value=mock_api):
            export_gold_candidates.run()

        # rtr_gold.json must be untouched
        assert gold_json_path.read_text() == original_content, (
            "export_gold_candidates.py must never write to rtr_gold.json"
        )

        # Only rtr_gold_candidates.json should be newly written
        candidates_path = tmp_path / "rtr_gold_candidates.json"
        assert candidates_path.exists()
