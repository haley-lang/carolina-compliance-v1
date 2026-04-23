"""
Email Classifier — Carolina Compliance Solutions
Classifies inbound emails into normalized event types before extraction.

Pass 1: Rule-based signals (sender, subject, attachment, body patterns)
Pass 2: Claude Haiku LLM classification when Pass 1 is inconclusive
"""

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

logger = logging.getLogger(__name__)

# ── Airtable field IDs: Incoming Documents ───────────────────────────────────
FLD_EVENT_TYPE               = "fldjorGTZS1bz6iWK"
FLD_CLASSIFICATION_CONFIDENCE = "fldhctMzK2wNpypdd"
FLD_CLASSIFICATION_METHOD    = "fldY4DAS9PSx0YE4l"
FLD_EMAIL_BODY_PREVIEW       = "fldOerG8InsQyC8Au"
FLD_STATUS                   = "fldIQxRQiAcc6tD6Z"

# ── Event types ──────────────────────────────────────────────────────────────
EVENT_NEW_COI             = "new_coi"
EVENT_RENEWAL_COI         = "renewal_coi"
EVENT_CANCELLATION        = "cancellation_notice"
EVENT_ENDORSEMENT         = "endorsement"
EVENT_REINSTATEMENT       = "reinstatement"
EVENT_BROKER_QUESTION     = "broker_question"
EVENT_DEFICIENCY_DISPUTE  = "deficiency_dispute"
EVENT_EXTENSION_REQUEST   = "extension_request"
EVENT_BINDER              = "binder"
EVENT_DECLARATION_PAGE    = "declaration_page"
EVENT_BOUNCE              = "bounce"
EVENT_AUTO_REPLY          = "auto_reply"
EVENT_NO_ATTACHMENT       = "no_attachment"
EVENT_CLOUD_LINK          = "cloud_link"
EVENT_DUPLICATE           = "duplicate"
EVENT_UNRELATED           = "unrelated"

# Events that should skip extraction entirely
SKIP_EVENTS = {EVENT_BOUNCE, EVENT_AUTO_REPLY, EVENT_UNRELATED, EVENT_NO_ATTACHMENT}

# Events that should be flagged for module_8b processing
MODULE_8B_EVENTS = {EVENT_CANCELLATION, EVENT_ENDORSEMENT, EVENT_REINSTATEMENT}

# ── Rule-based patterns ──────────────────────────────────────────────────────
BOUNCE_SENDER_PATTERNS = [
    r"mailer-daemon@",
    r"postmaster@",
    r"mail-daemon@",
    r"noreply.*bounce",
    r"maildelivery@",
]

AUTO_REPLY_SUBJECT_PATTERNS = [
    r"out of office",
    r"automatic reply",
    r"auto-reply",
    r"autoreply",
    r"i am out of the office",
    r"i'm out of the office",
    r"on vacation",
    r"away from the office",
    r"ooo:",
]

CANCELLATION_PATTERNS = [
    r"notice of cancellation",
    r"cancellation notice",
    r"policy cancellation",
    r"non-?renewal",
    r"intent to cancel",
    r"cancelled effective",
]

ENDORSEMENT_PATTERNS = [
    r"endorsement",
    r"policy change",
    r"amendment to policy",
    r"additional insured endorsement",
]

REINSTATEMENT_PATTERNS = [
    r"reinstatement",
    r"reinstated",
    r"policy restored",
    r"coverage restored",
]

BINDER_PATTERNS = [
    r"binder",
    r"temporary coverage",
    r"evidence of insurance",
    r"insurance binder",
]

DECLARATION_PATTERNS = [
    r"declaration page",
    r"dec page",
    r"declarations page",
    r"policy declarations",
]

DEFICIENCY_DISPUTE_PATTERNS = [
    r"disagree",
    r"dispute",
    r"incorrect",
    r"already (sent|provided|submitted)",
    r"this is wrong",
    r"please review again",
    r"we believe",
]

EXTENSION_REQUEST_PATTERNS = [
    r"extension",
    r"more time",
    r"additional time",
    r"working on getting",
    r"will send (soon|shortly|asap)",
    r"few more days",
]

CLOUD_LINK_PATTERNS = [
    r"https?://drive\.google\.com",
    r"https?://www\.dropbox\.com",
    r"https?://.*\.sharepoint\.com",
    r"https?://onedrive\.live\.com",
    r"https?://box\.com",
    r"https?://app\.docusign\.com",
    r"click here to (download|view|access)",
    r"download your certificate",
]

COI_FILENAME_PATTERNS = [
    r"coi",
    r"certificate",
    r"acord",
    r"insurance",
    r"liability",
    r"workers.?comp",
]

# ── Dataclass ────────────────────────────────────────────────────────────────

@dataclass
class ClassificationResult:
    event_type: str
    confidence: int          # 0-100
    method: str              # rule_based, llm, unclassified
    should_skip: bool
    flag_module_8b: bool


# ── Pass 1: Rule-based classification ────────────────────────────────────────

def _matches_any(text: str, patterns: list) -> bool:
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in patterns)


def _extract_body_text(msg) -> str:
    """Extract plain text body from an email.message.Message object."""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = part.get_content_disposition() or ""
            if content_type == "text/plain" and "attachment" not in disposition:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
        # Fallback to HTML if no plain text
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = part.get_content_disposition() or ""
            if content_type == "text/html" and "attachment" not in disposition:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html = payload.decode(charset, errors="replace")
                    return re.sub(r"<[^>]+>", " ", html)
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return ""


def classify_rule_based(
    sender: str,
    subject: str,
    body_text: str,
    attachment_filenames: list,
    has_attachments: bool,
) -> Optional[ClassificationResult]:
    """Pass 1: fast rule-based classification. Returns None if inconclusive."""

    sender_lower = sender.lower()
    subject_lower = subject.lower()
    combined_text = f"{subject_lower} {body_text.lower()}"

    # ── Bounce detection ─────────────────────────────────────────────────
    if _matches_any(sender, BOUNCE_SENDER_PATTERNS):
        return ClassificationResult(EVENT_BOUNCE, 100, "rule_based", True, False)
    if "delivery" in subject_lower and ("fail" in subject_lower or "undeliverable" in subject_lower):
        return ClassificationResult(EVENT_BOUNCE, 95, "rule_based", True, False)

    # ── Auto-reply detection ─────────────────────────────────────────────
    if _matches_any(subject, AUTO_REPLY_SUBJECT_PATTERNS):
        return ClassificationResult(EVENT_AUTO_REPLY, 95, "rule_based", True, False)
    if _matches_any(body_text, AUTO_REPLY_SUBJECT_PATTERNS):
        return ClassificationResult(EVENT_AUTO_REPLY, 85, "rule_based", True, False)

    # ── No attachment ────────────────────────────────────────────────────
    if not has_attachments and not _matches_any(body_text, CLOUD_LINK_PATTERNS):
        # Check if it's a question or dispute before marking no_attachment
        if _matches_any(combined_text, DEFICIENCY_DISPUTE_PATTERNS):
            return ClassificationResult(EVENT_DEFICIENCY_DISPUTE, 80, "rule_based", False, False)
        if _matches_any(combined_text, EXTENSION_REQUEST_PATTERNS):
            return ClassificationResult(EVENT_EXTENSION_REQUEST, 80, "rule_based", False, False)
        if "?" in subject or "?" in body_text[:500]:
            return ClassificationResult(EVENT_BROKER_QUESTION, 70, "rule_based", False, False)
        return ClassificationResult(EVENT_NO_ATTACHMENT, 90, "rule_based", True, False)

    # ── Cloud link detection ─────────────────────────────────────────────
    if not has_attachments and _matches_any(body_text, CLOUD_LINK_PATTERNS):
        return ClassificationResult(EVENT_CLOUD_LINK, 85, "rule_based", False, False)

    # ── Cancellation ─────────────────────────────────────────────────────
    if _matches_any(combined_text, CANCELLATION_PATTERNS):
        return ClassificationResult(EVENT_CANCELLATION, 90, "rule_based", False, True)

    # ── Endorsement ──────────────────────────────────────────────────────
    if _matches_any(combined_text, ENDORSEMENT_PATTERNS):
        # Check filenames too
        filenames_text = " ".join(attachment_filenames).lower()
        if "endorsement" in filenames_text or _matches_any(subject, ENDORSEMENT_PATTERNS):
            return ClassificationResult(EVENT_ENDORSEMENT, 90, "rule_based", False, True)
        return ClassificationResult(EVENT_ENDORSEMENT, 75, "rule_based", False, True)

    # ── Reinstatement ────────────────────────────────────────────────────
    if _matches_any(combined_text, REINSTATEMENT_PATTERNS):
        return ClassificationResult(EVENT_REINSTATEMENT, 90, "rule_based", False, True)

    # ── Binder ───────────────────────────────────────────────────────────
    filenames_lower = " ".join(attachment_filenames).lower()
    if _matches_any(combined_text, BINDER_PATTERNS) or "binder" in filenames_lower:
        return ClassificationResult(EVENT_BINDER, 85, "rule_based", False, False)

    # ── Declaration page ─────────────────────────────────────────────────
    if _matches_any(combined_text, DECLARATION_PATTERNS) or "dec" in filenames_lower:
        return ClassificationResult(EVENT_DECLARATION_PAGE, 80, "rule_based", False, False)

    # ── Deficiency dispute ───────────────────────────────────────────────
    if _matches_any(combined_text, DEFICIENCY_DISPUTE_PATTERNS):
        return ClassificationResult(EVENT_DEFICIENCY_DISPUTE, 75, "rule_based", False, False)

    # ── Extension request ────────────────────────────────────────────────
    if _matches_any(combined_text, EXTENSION_REQUEST_PATTERNS):
        return ClassificationResult(EVENT_EXTENSION_REQUEST, 75, "rule_based", False, False)

    # ── PDF attachment with COI-like filename → likely new_coi ───────────
    if has_attachments:
        has_pdf = any(f.lower().endswith(".pdf") for f in attachment_filenames)
        if has_pdf:
            if _matches_any(filenames_lower, COI_FILENAME_PATTERNS):
                return ClassificationResult(EVENT_NEW_COI, 85, "rule_based", False, False)
            # Has PDF but filename isn't obviously COI — inconclusive, go to Pass 2
            return None

    # Inconclusive
    return None


# ── Pass 2: LLM classification ───────────────────────────────────────────────

LLM_CLASSIFICATION_PROMPT = """You are an email classifier for a Certificate of Insurance (COI) compliance system. Classify this email into exactly one category.

Categories:
- new_coi: COI document attached or being submitted
- renewal_coi: updated/renewed COI being submitted (mentions renewal, updated certificate, new policy period)
- cancellation_notice: notice that a policy is being cancelled or not renewed
- endorsement: endorsement document (policy amendment, not a full COI)
- reinstatement: notice that a cancelled policy has been reinstated
- broker_question: broker or vendor asking a question, no actionable document
- deficiency_dispute: sender disputing or pushing back on a compliance finding
- extension_request: asking for more time to provide documents
- binder: temporary coverage binder, not a full COI
- declaration_page: policy declarations page, not a COI
- unrelated: not related to insurance compliance at all

Return ONLY a JSON object with two fields:
{"event_type": "<category>", "confidence": <0-100>}

Email details:
From: {sender}
Subject: {subject}
Body (first 500 chars): {body_preview}
Attachments: {attachments}"""


def classify_with_llm(
    sender: str,
    subject: str,
    body_text: str,
    attachment_filenames: list,
) -> ClassificationResult:
    """Pass 2: use Claude Haiku to classify ambiguous emails."""
    import json

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — cannot run LLM classification")
        return ClassificationResult(EVENT_NEW_COI, 50, "unclassified", False, False)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        body_preview = body_text[:500].strip()
        attachments_str = ", ".join(attachment_filenames) if attachment_filenames else "(none)"

        prompt = LLM_CLASSIFICATION_PROMPT.format(
            sender=sender,
            subject=subject,
            body_preview=body_preview,
            attachments=attachments_str,
        )

        from extractor import call_claude_with_retry
        response = call_claude_with_retry(
            client.messages.create,
            model="claude-haiku-4-5-20251001",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0,
        )

        raw = response.content[0].text.strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        result = json.loads(raw)
        event_type = result.get("event_type", EVENT_NEW_COI)
        confidence = min(max(int(result.get("confidence", 70)), 0), 100)

        # Validate event_type is one we recognize
        valid_types = {
            EVENT_NEW_COI, EVENT_RENEWAL_COI, EVENT_CANCELLATION, EVENT_ENDORSEMENT,
            EVENT_REINSTATEMENT, EVENT_BROKER_QUESTION, EVENT_DEFICIENCY_DISPUTE,
            EVENT_EXTENSION_REQUEST, EVENT_BINDER, EVENT_DECLARATION_PAGE,
            EVENT_BOUNCE, EVENT_AUTO_REPLY, EVENT_NO_ATTACHMENT, EVENT_CLOUD_LINK,
            EVENT_DUPLICATE, EVENT_UNRELATED,
        }
        if event_type not in valid_types:
            logger.warning("LLM returned unknown event_type '%s' — defaulting to new_coi", event_type)
            event_type = EVENT_NEW_COI
            confidence = 50

        should_skip = event_type in SKIP_EVENTS
        flag_8b = event_type in MODULE_8B_EVENTS

        logger.info("LLM classification: %s (confidence=%d)", event_type, confidence)
        return ClassificationResult(event_type, confidence, "llm", should_skip, flag_8b)

    except Exception as exc:
        logger.error("LLM classification failed: %s — defaulting to new_coi", exc)
        return ClassificationResult(EVENT_NEW_COI, 50, "unclassified", False, False)


# ── Main classifier entry point ──────────────────────────────────────────────

def classify_email(
    msg,
    sender: str,
    subject: str,
    attachment_filenames: list,
    has_attachments: bool,
) -> ClassificationResult:
    """Classify an email in two passes. Returns a ClassificationResult.

    Args:
        msg: email.message.Message object (for body extraction)
        sender: decoded sender string
        subject: decoded subject string
        attachment_filenames: list of attachment filename strings
        has_attachments: whether any qualifying attachments were found
    """
    body_text = _extract_body_text(msg)

    # Pass 1: rule-based
    result = classify_rule_based(sender, subject, body_text, attachment_filenames, has_attachments)

    if result is not None:
        logger.info("Email classified (rule-based): %s (confidence=%d) sender=%s subject=%s",
                     result.event_type, result.confidence, sender[:50], subject[:80])
        result._body_preview = body_text[:500]
        return result

    # Pass 2: LLM
    logger.info("Rule-based inconclusive — running LLM classification for: %s", subject[:80])
    result = classify_with_llm(sender, subject, body_text, attachment_filenames)
    result._body_preview = body_text[:500]
    return result


def write_classification_to_airtable(table, record_id: str, result: ClassificationResult):
    """Write classification fields to the Incoming Documents record."""
    fields = {
        FLD_EVENT_TYPE: result.event_type,
        FLD_CLASSIFICATION_CONFIDENCE: result.confidence,
        FLD_CLASSIFICATION_METHOD: result.method,
        FLD_EMAIL_BODY_PREVIEW: getattr(result, "_body_preview", "")[:500],
    }

    if result.should_skip:
        fields[FLD_STATUS] = "Skipped"

    try:
        table.update(record_id, fields, typecast=True)
        logger.info("Classification written to Airtable: record=%s event_type=%s confidence=%d method=%s skip=%s",
                     record_id, result.event_type, result.confidence, result.method, result.should_skip)
    except Exception as exc:
        logger.error("Failed to write classification to Airtable for %s: %s", record_id, exc)
