"""
Compliance guard for marketing copy.
Every draft must pass through check() before being shown or stored.
Hard rules from MARKETING_BUILD_PLAN.md §Hard rules.
"""
from __future__ import annotations
import re

_BANNED_WORDS = (
    "compliant",
    "compliance",  # allowed in company name context only — see _COMPANY_NAME_RE
    "verified",
    "covered",
    "certified",
)

# Phrases that imply auditors demand every policy term or look at past years
_AUDIT_CLAIM_PATTERNS = (
    re.compile(r"\bauditors?\s+(require|demand|look\s+at|check)\s+(all|every|past|prior|previous)\b", re.I),
    re.compile(r"\bauditors?\s+require\s+every\s+policy\s+term\b", re.I),
    re.compile(r"\bauditors?\s+review\s+past\s+year", re.I),
    re.compile(r"\bauditors?\s+ask\s+for\s+(all|every|past|prior)\b", re.I),
    re.compile(r"\bpast\s+year[s']?\s+(of\s+)?policy\s+(terms?|records?)\s+are\s+required\b", re.I),
    re.compile(r"\baverage\s+percent\s+compliant\b", re.I),
    re.compile(r"\bpercent\s+(compliant|verified|covered)\b", re.I),
    re.compile(r"\b\d+\s*%\s*(compliant|verified|covered)\b", re.I),
)

# State-specific legal or audit-rule claim patterns
_STATE_CLAIM_PATTERNS = (
    re.compile(
        r"\b(in\s+)?(north carolina|south carolina|florida|texas|california|new york|georgia"
        r"|virginia|tennessee|ohio|pennsylvania|illinois|michigan)\s+(law|regulation|statute"
        r"|code|rule|require[sd]?|mandate[sd]?|audit)\b",
        re.I,
    ),
    re.compile(r"\bstate\s+law\s+requires?\b", re.I),
    re.compile(r"\bunder\s+\w+\s+state\s+law\b", re.I),
    re.compile(r"\bNCGS\b"),          # NC General Statutes citation
    re.compile(r"\bOSHA\s+\d+\b", re.I),  # state-specific OSHA citations
)

# CCS company-name usage — these are allowed even though they contain "compliance"
_COMPANY_NAME_RE = re.compile(
    r"\bcarolina\s+compliance\s+solutions?\b"
    r"|\bccs\b"
    r"|\bcarolina\s+compliance\b",
    re.I,
)


def _strip_company_name(text: str) -> str:
    """Replace company-name occurrences with a placeholder before banned-word scan."""
    return _COMPANY_NAME_RE.sub("__CCS__", text)


class GuardResult:
    __slots__ = ("ok", "flags")

    def __init__(self, ok: bool, flags: list[str]):
        self.ok = ok
        self.flags = flags

    def __bool__(self) -> bool:
        return self.ok

    def __repr__(self) -> str:
        status = "PASS" if self.ok else "FAIL"
        return f"GuardResult({status}, flags={self.flags!r})"


def check(text: str) -> GuardResult:
    """
    Run all checks on *text*.  Returns GuardResult(ok=True) if clean.

    Checks (in order):
      1. Banned words (compliant, verified, covered, certified, compliance outside company name)
      2. Audit-claim phrases
      3. State-specific legal/audit-rule claims
    """
    flags: list[str] = []
    scrubbed = _strip_company_name(text)

    for word in _BANNED_WORDS:
        pattern = re.compile(r"\b" + re.escape(word) + r"\b", re.I)
        if pattern.search(scrubbed):
            flags.append(f"banned-word:{word}")

    for pat in _AUDIT_CLAIM_PATTERNS:
        if pat.search(text):
            flags.append(f"audit-claim:{pat.pattern[:60]}")

    for pat in _STATE_CLAIM_PATTERNS:
        if pat.search(text):
            flags.append(f"state-claim:{pat.pattern[:60]}")

    return GuardResult(ok=len(flags) == 0, flags=flags)


def assert_clean(text: str) -> str:
    """Raise ValueError if text fails the guard. Returns text unchanged if clean."""
    result = check(text)
    if not result.ok:
        raise ValueError(f"Marketing copy failed compliance guard: {result.flags}")
    return text
