import re
import logging
from datetime import datetime
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)


def safe_filename(filename: str) -> str:
    """
    Strip characters that are unsafe in file system paths and
    collapse whitespace so saved files have clean names.
    """
    filename = filename.strip()
    filename = re.sub(r"[^\w\s\-.]", "_", filename)
    filename = re.sub(r"\s+", "_", filename)
    return filename


def parse_email_date(date_str: str) -> str:
    """
    Parse an RFC 2822 email Date header into an ISO 8601 string.
    Returns the original string unchanged if parsing fails.
    """
    if not date_str:
        return ""
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.isoformat()
    except Exception:
        logger.warning("Could not parse date: %s", date_str)
        return date_str


# ── Holder name normalization ─────────────────────────────────────────────────

_HOLDER_STRIP_SUFFIXES = {
    "llc", "inc", "corp", "co", "ltd", "dba", "d/b/a",
    "company", "companies", "group", "holdings", "enterprises",
    "solutions", "services", "construction", "contracting",
    "builders", "building",
}


def normalize_holder_name(raw_name) -> str:
    """Normalize a certificate holder / company name for fuzzy comparison.

    Pure function, no external dependencies.
    Steps: lowercase → strip → remove punctuation except spaces →
    collapse spaces → remove legal/industry suffixes as whole words.
    """
    text = str(raw_name) if raw_name else ""
    text = text.lower().strip()
    # Remove all punctuation except spaces
    text = re.sub(r"[^\w\s]", " ", text)
    # Collapse multiple spaces
    text = " ".join(text.split())
    # Remove known suffixes as whole words (iteratively)
    words = text.split()
    changed = True
    while changed and words:
        changed = False
        if words[-1] in _HOLDER_STRIP_SUFFIXES:
            words.pop()
            changed = True
    return " ".join(words)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with a timestamped console handler."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
