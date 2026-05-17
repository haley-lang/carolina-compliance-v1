"""Unit tests for review_gate.compute_review_status and apply_onboarding_window_gate."""
from unittest.mock import MagicMock

import pytest

from review_gate import (
    compute_review_status,
    apply_onboarding_window_gate,
    REVIEW_STATUS_AUTO_APPROVED,
    REVIEW_STATUS_PENDING_REVIEW,
    REVIEW_REASON_NA,
    REVIEW_REASON_LOW_CONFIDENCE,
    REVIEW_REASON_ONBOARDING_WINDOW,
    REVIEW_REASON_BOTH,
    REVIEW_REASON_POSSIBLE_DUPLICATE,
    PROCESSING_STATUS_IMPORTED,
    PROCESSING_STATUS_PENDING_REVIEW,
)


# ── compute_review_status — 12 tests ─────────────────────────────────────────


def test_auto_approved_high_confidence_no_duplicate():
    assert compute_review_status(confidence=0.99, is_possible_duplicate=False) == (
        REVIEW_STATUS_AUTO_APPROVED, REVIEW_REASON_NA, PROCESSING_STATUS_IMPORTED,
    )


def test_auto_approved_at_exact_threshold():
    """0.95 == 0.95 should pass (uses >=, not >)."""
    assert compute_review_status(confidence=0.95, is_possible_duplicate=False, threshold=0.95) == (
        REVIEW_STATUS_AUTO_APPROVED, REVIEW_REASON_NA, PROCESSING_STATUS_IMPORTED,
    )


def test_low_confidence_just_below_threshold():
    assert compute_review_status(confidence=0.949, is_possible_duplicate=False, threshold=0.95) == (
        REVIEW_STATUS_PENDING_REVIEW, REVIEW_REASON_LOW_CONFIDENCE, PROCESSING_STATUS_PENDING_REVIEW,
    )


def test_low_confidence_none_value():
    """confidence=None → Low Confidence (fail-safe)."""
    assert compute_review_status(confidence=None, is_possible_duplicate=False) == (
        REVIEW_STATUS_PENDING_REVIEW, REVIEW_REASON_LOW_CONFIDENCE, PROCESSING_STATUS_PENDING_REVIEW,
    )


def test_low_confidence_zero():
    """0.0 (triage path) → Low Confidence."""
    assert compute_review_status(confidence=0.0, is_possible_duplicate=False) == (
        REVIEW_STATUS_PENDING_REVIEW, REVIEW_REASON_LOW_CONFIDENCE, PROCESSING_STATUS_PENDING_REVIEW,
    )


def test_low_confidence_non_numeric():
    """Defensive: 'high' string → Low Confidence."""
    assert compute_review_status(confidence="high", is_possible_duplicate=False) == (
        REVIEW_STATUS_PENDING_REVIEW, REVIEW_REASON_LOW_CONFIDENCE, PROCESSING_STATUS_PENDING_REVIEW,
    )


def test_low_confidence_bool_excluded():
    """bool is int subclass; True should NOT coerce to 1.0 and auto-approve."""
    assert compute_review_status(confidence=True, is_possible_duplicate=False) == (
        REVIEW_STATUS_PENDING_REVIEW, REVIEW_REASON_LOW_CONFIDENCE, PROCESSING_STATUS_PENDING_REVIEW,
    )


def test_possible_duplicate_takes_precedence_over_low_confidence():
    assert compute_review_status(confidence=0.5, is_possible_duplicate=True) == (
        REVIEW_STATUS_PENDING_REVIEW, REVIEW_REASON_POSSIBLE_DUPLICATE, PROCESSING_STATUS_PENDING_REVIEW,
    )


def test_possible_duplicate_takes_precedence_over_auto_approved():
    assert compute_review_status(confidence=0.99, is_possible_duplicate=True) == (
        REVIEW_STATUS_PENDING_REVIEW, REVIEW_REASON_POSSIBLE_DUPLICATE, PROCESSING_STATUS_PENDING_REVIEW,
    )


def test_explicit_threshold_kwarg_overrides_default():
    """Lowering threshold lets a 0.85-confidence record auto-approve."""
    assert compute_review_status(confidence=0.85, is_possible_duplicate=False, threshold=0.80) == (
        REVIEW_STATUS_AUTO_APPROVED, REVIEW_REASON_NA, PROCESSING_STATUS_IMPORTED,
    )


def test_threshold_default_from_config(monkeypatch):
    """When threshold kwarg is None, reads config.COI_REVIEW_CONFIDENCE_THRESHOLD."""
    import config
    monkeypatch.setattr(config, "COI_REVIEW_CONFIDENCE_THRESHOLD", 0.80)
    assert compute_review_status(confidence=0.85, is_possible_duplicate=False) == (
        REVIEW_STATUS_AUTO_APPROVED, REVIEW_REASON_NA, PROCESSING_STATUS_IMPORTED,
    )


def test_compute_review_status_handles_all_inputs_none_or_default():
    """Happy-path sanity: explicit happy-path inputs return Auto-Approved/N/A/Imported.

    Note: confidence and is_possible_duplicate are required positional args
    (no implicit defaults — forcing callers to think about both is safer).
    This test asserts the canonical happy-path return value.
    """
    assert compute_review_status(confidence=1.0, is_possible_duplicate=False) == (
        REVIEW_STATUS_AUTO_APPROVED, REVIEW_REASON_NA, PROCESSING_STATUS_IMPORTED,
    )


# ── apply_onboarding_window_gate — 4 tests ───────────────────────────────────


def _make_client(client_id: str, review_mode: str = None) -> dict:
    fields = {}
    if review_mode is not None:
        fields["Review Mode"] = review_mode
    return {"id": client_id, "fields": fields}


def test_downgrade_auto_approved_to_pending_review_when_client_in_manual_window():
    incoming_table = MagicMock()
    clients = [_make_client("cliABC", "Manual Review Window")]
    extraction_fields = {
        "Processing Status": "Imported",
        "Review Status": "Auto-Approved",
        "Review Reason": "N/A",
    }
    result = apply_onboarding_window_gate(
        incoming_table=incoming_table,
        client_records=clients,
        extraction_id="recXYZ",
        extraction_fields=extraction_fields,
        matched_client_id="cliABC",
    )
    assert result is True  # caller skips downstream writes
    incoming_table.update.assert_called_once_with("recXYZ", {
        "Review Status": "Pending Review",
        "Review Reason": "Onboarding Window",
        "Processing Status": "Pending Review",
    }, typecast=True)


def test_upgrade_low_confidence_to_both_when_client_in_manual_window():
    incoming_table = MagicMock()
    clients = [_make_client("cliABC", "Manual Review Window")]
    extraction_fields = {
        "Processing Status": "Pending Review",
        "Review Status": "Pending Review",
        "Review Reason": "Low Confidence",
    }
    result = apply_onboarding_window_gate(
        incoming_table=incoming_table,
        client_records=clients,
        extraction_id="recXYZ",
        extraction_fields=extraction_fields,
        matched_client_id="cliABC",
    )
    assert result is True
    incoming_table.update.assert_called_once_with("recXYZ", {
        "Review Reason": "Both",
    }, typecast=True)


def test_no_change_when_client_in_threshold_review():
    """Threshold Review client: Imported records proceed downstream (gate returns False).

    Pending Review records would still skip downstream (return True), but
    that's tested separately."""
    incoming_table = MagicMock()
    clients = [_make_client("cliABC", "Threshold Review")]
    extraction_fields = {
        "Processing Status": "Imported",
        "Review Status": "Auto-Approved",
        "Review Reason": "N/A",
    }
    result = apply_onboarding_window_gate(
        incoming_table=incoming_table,
        client_records=clients,
        extraction_id="recXYZ",
        extraction_fields=extraction_fields,
        matched_client_id="cliABC",
    )
    assert result is False  # caller proceeds with downstream writes
    incoming_table.update.assert_not_called()


def test_no_change_when_client_has_no_review_mode():
    """Legacy client without Review Mode field — treated as Threshold Review."""
    incoming_table = MagicMock()
    clients = [_make_client("cliLegacy", review_mode=None)]
    extraction_fields = {
        "Processing Status": "Imported",
        "Review Status": "Auto-Approved",
        "Review Reason": "N/A",
    }
    result = apply_onboarding_window_gate(
        incoming_table=incoming_table,
        client_records=clients,
        extraction_id="recXYZ",
        extraction_fields=extraction_fields,
        matched_client_id="cliLegacy",
    )
    assert result is False
    incoming_table.update.assert_not_called()


def test_gate_skips_writes_for_pending_review_when_no_client_matched():
    """Pending Review record with no client match → skip downstream writes anyway."""
    incoming_table = MagicMock()
    extraction_fields = {
        "Processing Status": "Pending Review",
        "Review Status": "Pending Review",
        "Review Reason": "Low Confidence",
    }
    result = apply_onboarding_window_gate(
        incoming_table=incoming_table,
        client_records=[],
        extraction_id="recXYZ",
        extraction_fields=extraction_fields,
        matched_client_id=None,
    )
    assert result is True
    incoming_table.update.assert_not_called()
