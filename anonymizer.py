"""
Anonymizer for marketing copy.
Strips client names, vendor names, policy numbers, carrier names, and addresses
from text before it is shown publicly or passed to Claude for story drafting.

Usage:
    from anonymizer import anonymize
    clean = anonymize(text, known_names=["Acme GC", "State Farm"])
"""
from __future__ import annotations
import re

# ── Generic redaction patterns ────────────────────────────────────────────────

# Policy numbers: letter(s) + digits, common formats like GL-1234567, WC-12-345-6789
# No space allowed between letters and digits to avoid matching "FEIN 12-..." as a policy number.
_POLICY_NUMBER_RE = re.compile(
    r"\b([A-Z]{1,5}-?\d[\d\-]{4,20})\b"
)

# Carrier / insurance company names — common ones plus a generic pattern
_KNOWN_CARRIERS = (
    "State Farm", "Travelers", "Hartford", "Liberty Mutual", "Nationwide",
    "Zurich", "CNA", "Markel", "Employers", "Berkley", "Hiscox",
    "Philadelphia Indemnity", "Westfield", "Erie", "Auto-Owners", "Acuity",
    "Chubb", "AIG", "Tokio Marine", "Hanover", "Merchants",
    "Great American", "Bitco", "Guard Insurance", "AmTrust", "Berkshire Hathaway",
    "EMPLOYERS", "Preferred Employers",
)

_CARRIER_RE = re.compile(
    r"\b(" + "|".join(re.escape(c) for c in _KNOWN_CARRIERS) + r")\b",
    re.I,
)

# Street addresses: number + street name + suffix
_ADDRESS_RE = re.compile(
    r"\b\d{1,5}\s+[A-Z][a-zA-Z\s]{2,40}"
    r"\s+(St|Street|Ave|Avenue|Blvd|Boulevard|Rd|Road|Dr|Drive|Ln|Lane|Way|Ct|Court|Pl|Place)"
    r"\.?\b",
    re.I,
)

# City, State ZIP combos
_CITY_STATE_ZIP_RE = re.compile(
    r"\b[A-Z][a-zA-Z\s]{2,25},\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?\b"
)

# EIN / FEIN patterns
_EIN_RE = re.compile(r"\b\d{2}-\d{7}\b")

# Email addresses
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")

# Phone numbers
_PHONE_RE = re.compile(
    r"\b(?:\+1\s?)?(?:\(?\d{3}\)?[\s.\-]?)?\d{3}[\s.\-]\d{4}\b"
)


def anonymize(
    text: str,
    known_names: list[str] | None = None,
) -> str:
    """
    Return *text* with identifying information replaced by placeholders.

    Args:
        text: Raw text that may contain PII or business identifiers.
        known_names: Extra names (client, vendor, person) to redact.
                     Matched case-insensitively as whole words when possible.
    """
    if not text:
        return text

    result = text

    # Redact caller-supplied names first (most specific)
    if known_names:
        for name in known_names:
            if not name or not name.strip():
                continue
            pattern = re.compile(r"\b" + re.escape(name.strip()) + r"\b", re.I)
            result = pattern.sub("[Contractor]", result)

    # Carriers
    result = _CARRIER_RE.sub("[Carrier]", result)

    # Policy numbers (after carriers so carrier abbreviations don't interfere)
    result = _POLICY_NUMBER_RE.sub("[Policy#]", result)

    # Addresses
    result = _ADDRESS_RE.sub("[Address]", result)
    result = _CITY_STATE_ZIP_RE.sub("[City, ST ZIP]", result)

    # EIN
    result = _EIN_RE.sub("[EIN]", result)

    # Contact info
    result = _EMAIL_RE.sub("[email]", result)
    result = _PHONE_RE.sub("[phone]", result)

    return result


def redact_names(text: str, names: list[str]) -> str:
    """Convenience wrapper: redact only the given names, no other patterns."""
    if not text or not names:
        return text
    result = text
    for name in names:
        if name and name.strip():
            pat = re.compile(r"\b" + re.escape(name.strip()) + r"\b", re.I)
            result = pat.sub("[Contractor]", result)
    return result
