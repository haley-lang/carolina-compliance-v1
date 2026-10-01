"""Verify that fetch_all_pending_extractions uses the renamed Processing Status values.

This test fails against the old formula ({Processing Status} = 'Pending Review')
and passes with the new formula ({Processing Status} = 'Low Confidence').
"""
from unittest.mock import MagicMock

from processor import fetch_all_pending_extractions


def test_fetch_formula_uses_low_confidence_not_pending_review():
    """Formula must filter on 'Low Confidence', not the old catch-all 'Pending Review'."""
    mock_table = MagicMock()
    mock_table.all.return_value = []

    fetch_all_pending_extractions(mock_table)

    formula = mock_table.all.call_args[1]["formula"]
    assert "Low Confidence" in formula, (
        f"formula must reference 'Low Confidence' for the renamed status; got: {formula!r}"
    )
    assert "'Pending Review'" not in formula, (
        f"formula must not reference the old 'Pending Review' Processing Status; got: {formula!r}"
    )


def test_fetch_formula_includes_imported():
    """Formula must still pull Imported records (the happy-path auto-approved case)."""
    mock_table = MagicMock()
    mock_table.all.return_value = []

    fetch_all_pending_extractions(mock_table)

    formula = mock_table.all.call_args[1]["formula"]
    assert "Imported" in formula, f"formula must include 'Imported'; got: {formula!r}"


def test_fetch_formula_excludes_duplicate_and_needs_review():
    """Formula must not pull Duplicate or Needs Review records (terminal blocking states)."""
    mock_table = MagicMock()
    mock_table.all.return_value = []

    fetch_all_pending_extractions(mock_table)

    formula = mock_table.all.call_args[1]["formula"]
    assert "'Duplicate'" not in formula, (
        f"formula must not reference 'Duplicate'; got: {formula!r}"
    )
    assert "'Needs Review'" not in formula, (
        f"formula must not reference 'Needs Review'; got: {formula!r}"
    )
