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


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with a timestamped console handler."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
