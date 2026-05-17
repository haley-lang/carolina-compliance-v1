"""Review gating for the COI Review Dashboard project (Phase 1E).

Two pure-ish functions:

  compute_review_status(confidence, is_possible_duplicate, threshold=None)
    Intake-time decision. Called from airtable_importer.build_fields right
    before a new Incoming Extractions record is created. Inspects the
    Claude-returned confidence + the duplicate check result, returns the
    (Review Status, Review Reason, Processing Status) triple to write.

  apply_onboarding_window_gate(incoming_table, client_records,
                                extraction_id, extraction_fields,
                                matched_client_id) -> bool
    Processor-time gate. Called from processor._process_single_extraction
    after client match succeeds, before downstream writes start. If the
    matched client is in "Manual Review Window" mode, downgrades
    Auto-Approved records to Pending Review / Onboarding Window, or
    upgrades Pending Review / Low Confidence records to "Both" reason.
    Returns True if the caller should SKIP downstream writes (record is
    Pending Review or was just downgraded).

Precedence (compute_review_status):
    1. is_possible_duplicate → Possible Duplicate (trumps everything)
    2. confidence missing or < threshold → Low Confidence
    3. otherwise → Auto-Approved / N/A / Imported

The "Both" combination (Low Confidence + Onboarding Window) is decided
in apply_onboarding_window_gate, NOT in compute_review_status — client
match hasn't happened at intake time.
"""

import logging

logger = logging.getLogger(__name__)


# Review Status values (single-select on Incoming Extractions)
REVIEW_STATUS_AUTO_APPROVED = "Auto-Approved"
REVIEW_STATUS_PENDING_REVIEW = "Pending Review"
REVIEW_STATUS_APPROVED = "Approved"
REVIEW_STATUS_APPROVED_EDITED = "Approved + Edited"
REVIEW_STATUS_REJECTED = "Rejected"
REVIEW_STATUS_ESCALATED = "Escalated to GC"

# Review Reason values (single-select on Incoming Extractions)
REVIEW_REASON_NA = "N/A"
REVIEW_REASON_LOW_CONFIDENCE = "Low Confidence"
REVIEW_REASON_ONBOARDING_WINDOW = "Onboarding Window"
REVIEW_REASON_BOTH = "Both"
REVIEW_REASON_POSSIBLE_DUPLICATE = "Possible Duplicate"

# Processing Status values (single-select on Incoming Extractions)
PROCESSING_STATUS_IMPORTED = "Imported"
PROCESSING_STATUS_PENDING_REVIEW = "Pending Review"

# Client Review Mode values (single-select on Clients)
REVIEW_MODE_MANUAL_WINDOW = "Manual Review Window"
REVIEW_MODE_THRESHOLD = "Threshold Review"


def compute_review_status(
    confidence,
    is_possible_duplicate: bool,
    threshold=None,
) -> tuple:
    """Decide (review_status, review_reason, processing_status) at intake.

    Args:
        confidence: overall confidence float (0.0–1.0) from extractor JSON.
            None / non-numeric / bool treated as missing → Low Confidence
            (fail-safe: when in doubt, send to review).
        is_possible_duplicate: True if check_duplicate_extraction returned
            a matching prior record.
        threshold: confidence floor for auto-approve. None → reads
            config.COI_REVIEW_CONFIDENCE_THRESHOLD (default 0.95).

    Returns:
        Tuple of three strings: (review_status, review_reason, processing_status).

    Precedence (highest to lowest):
        1. is_possible_duplicate → Possible Duplicate
        2. confidence missing or < threshold → Low Confidence
        3. otherwise → Auto-Approved
    """
    # Possible Duplicate trumps everything else
    if is_possible_duplicate:
        return (
            REVIEW_STATUS_PENDING_REVIEW,
            REVIEW_REASON_POSSIBLE_DUPLICATE,
            PROCESSING_STATUS_PENDING_REVIEW,
        )

    # Resolve threshold (env var → config default)
    if threshold is None:
        import config
        threshold = config.COI_REVIEW_CONFIDENCE_THRESHOLD

    # Confidence presence + numeric check (exclude bool; bool is int subclass)
    is_numeric = isinstance(confidence, (int, float)) and not isinstance(confidence, bool)
    if not is_numeric or confidence < threshold:
        return (
            REVIEW_STATUS_PENDING_REVIEW,
            REVIEW_REASON_LOW_CONFIDENCE,
            PROCESSING_STATUS_PENDING_REVIEW,
        )

    return (
        REVIEW_STATUS_AUTO_APPROVED,
        REVIEW_REASON_NA,
        PROCESSING_STATUS_IMPORTED,
    )


def apply_onboarding_window_gate(
    incoming_table,
    client_records,
    extraction_id: str,
    extraction_fields: dict,
    matched_client_id,
) -> bool:
    """Apply Manual Review Window downgrade/upgrade. Decide whether processor
    should skip downstream writes.

    Returns True if the caller should SKIP downstream writes (record is
    Pending Review now, either pre-existing or just downgraded). Returns
    False if processing should proceed to certificate/policy creation.

    Side effects (writes to incoming_table):
        - Auto-Approved + client Manual Window → downgrade to Pending
          Review / Onboarding Window / Pending Review.
        - Pending Review + Low Confidence + client Manual Window → upgrade
          Review Reason to "Both".
        - Otherwise: no write.

    Cases handled:
        1. No matched_client_id → can't apply per-client mode.
           If record was already Pending Review, skip writes (return True).
           If Imported, proceed (return False) — Auto-Approved without a
           known client is the right fall-through.
        2. matched_client_id but client not in client_records → same as (1).
        3. Client has no Review Mode field or unset → treat as Threshold
           Review (most permissive). Same dispatch as (1).
        4. Client Review Mode = "Threshold Review" → same as (3).
        5. Client Review Mode = "Manual Review Window":
           a. Review Status = "Auto-Approved" → downgrade, return True.
           b. Review Status = "Pending Review" + Reason = "Low Confidence"
              → upgrade Reason to "Both", return True.
           c. Anything else (already-downgraded, Possible Duplicate, etc.)
              → no write, return True (still skip downstream).
    """
    processing_status = (extraction_fields.get("Processing Status") or "").strip()
    review_status = (extraction_fields.get("Review Status") or "").strip()
    review_reason = (extraction_fields.get("Review Reason") or "").strip()

    is_pending_review_record = (processing_status == PROCESSING_STATUS_PENDING_REVIEW)

    # No client → no per-client mode to apply
    if not matched_client_id:
        return is_pending_review_record

    matched_client = next(
        (r for r in client_records if r.get("id") == matched_client_id),
        None,
    )
    if not matched_client:
        return is_pending_review_record

    review_mode = (matched_client.get("fields", {}).get("Review Mode") or "").strip()

    if review_mode != REVIEW_MODE_MANUAL_WINDOW:
        # Threshold Review (or unset). Proceed if Imported, skip if Pending Review.
        return is_pending_review_record

    # Manual Review Window — apply downgrade/upgrade
    if review_status == REVIEW_STATUS_AUTO_APPROVED:
        try:
            incoming_table.update(extraction_id, {
                "Review Status": REVIEW_STATUS_PENDING_REVIEW,
                "Review Reason": REVIEW_REASON_ONBOARDING_WINDOW,
                "Processing Status": PROCESSING_STATUS_PENDING_REVIEW,
            }, typecast=True)
            logger.info(
                "[review-gate] Downgraded %s: Auto-Approved -> Pending Review (Onboarding Window) "
                "(client in Manual Review Window)",
                extraction_id,
            )
        except Exception as exc:
            logger.error("[review-gate] Failed to downgrade %s: %s", extraction_id, exc)
            # Even if write fails, instruct caller to skip downstream — we
            # don't want to create policy/cert records for a record that
            # SHOULD be Pending Review.
        return True

    if review_status == REVIEW_STATUS_PENDING_REVIEW and review_reason == REVIEW_REASON_LOW_CONFIDENCE:
        try:
            incoming_table.update(extraction_id, {
                "Review Reason": REVIEW_REASON_BOTH,
            }, typecast=True)
            logger.info(
                "[review-gate] Upgraded %s reason: Low Confidence -> Both "
                "(client in Manual Review Window)",
                extraction_id,
            )
        except Exception as exc:
            logger.error("[review-gate] Failed to upgrade reason on %s: %s", extraction_id, exc)
        return True

    # Pending Review for some other reason (Possible Duplicate already
    # filtered out earlier, but be defensive) — skip downstream.
    return True
