"""
UTM tracked-link builder for marketing content.
Generates unique UTM-parameterized URLs for the CCS website, demo page, and Audit Prep page.
One link per content piece; content_id is a short slug derived from the idea.
"""
from __future__ import annotations
import hashlib
import re
import urllib.parse

from marketing_constants import UTM_BASE_WEBSITE, UTM_BASE_DEMO, UTM_BASE_AUDIT

PLATFORM_MEDIUM = {
    "TikTok": "social",
    "Facebook": "social",
    "LinkedIn": "social",
    "Email": "email",
    "Website FAQ": "web",
    "linkedin_personal": "social",
    "ccs_facebook": "social",
    "ccs_tiktok": "social",
    "local_facebook_group": "social",
    "contractor_facebook_group": "social",
}

UTM_CAMPAIGN = "ccs-mktg"


def _slug(text: str, max_len: int = 32) -> str:
    """Turn idea text into a short URL-safe slug."""
    s = text.lower()
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    s = s[:max_len].rstrip("-")
    return s or "post"


def _unique_id(idea_text: str, platform: str) -> str:
    """Short 6-char hash to make each link unique even if slug collides."""
    h = hashlib.sha1(f"{idea_text}:{platform}".encode()).hexdigest()[:6]
    return h


def build_utm_link(
    idea_text: str,
    platform: str,
    page: str = "home",
    cta_type: str | None = None,
) -> str:
    """
    Build a UTM URL for a given content piece.

    Args:
        idea_text: The idea or draft text (used to generate the content slug).
        platform: Channel name (linkedin_personal, ccs_facebook, etc.) or legacy value.
        page: 'home', 'demo', or 'audit' — which CCS page to link to.
        cta_type: Optional CTA variant ('message_me' or 'sign_up') — stored as utm_term.

    Returns:
        Full URL with UTM params.
    """
    base_map = {
        "home": UTM_BASE_WEBSITE,
        "demo": UTM_BASE_DEMO,
        "audit": UTM_BASE_AUDIT,
    }
    base_url = base_map.get(page, UTM_BASE_WEBSITE)
    medium = PLATFORM_MEDIUM.get(platform, "social")
    source = platform.lower().replace(" ", "-")
    slug = _slug(idea_text)
    uid = _unique_id(idea_text, platform)
    content_param = f"{slug}-{uid}"

    p: dict[str, str] = {
        "utm_source":   source,
        "utm_medium":   medium,
        "utm_campaign": UTM_CAMPAIGN,
        "utm_content":  content_param,
    }
    if cta_type:
        p["utm_term"] = cta_type

    params = urllib.parse.urlencode(p)
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}{params}"
