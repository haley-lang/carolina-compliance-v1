"""
Stage 2 content generator.
Takes one approved Marketing Idea and produces a full content set:
  - TikTok/Facebook talking points (3-4 bullets + 3 hooks)
  - LinkedIn post (+ 3 hooks)
  - Facebook post (+ 3 hooks)
  - Prospect email draft (+ 3 subject hooks)
  - Website FAQ entry (+ 3 question phrasings)

Each piece goes to Content Queue as Draft with guard result and UTM link.
No sending or posting — draft only.
"""
from __future__ import annotations
import json
import logging
import os
from datetime import datetime, timezone

import anthropic
import pyairtable

from compliance_guard import check as guard_check
from anonymizer import anonymize
from utm_builder import build_utm_link
from marketing_constants import (
    MARKETING_BASE_ID,
    CONTENT_QUEUE_TABLE_ID,
    CQ_DRAFT_TEXT, CQ_PLATFORM, CQ_HOOKS, CQ_STATUS, CQ_NEEDS_BILLY,
    CQ_GUARD_RESULT, CQ_TRACKED_LINK, CQ_IDEA_LINK,
    CQ_STATUS_DRAFT,
    MI_IDEA, MI_WHY_STORY, MI_AUDIENCE, MI_TOPIC,
)

log = logging.getLogger(__name__)

HAIKU_MODEL = "claude-haiku-4-5-20251001"

UTM_PAGE_MAP = {
    "TikTok":       "home",
    "Facebook":     "home",
    "LinkedIn":     "home",
    "Email":        "demo",
    "Website FAQ":  "audit",
}

_CONTENT_PROMPT = """\
You are helping Haley, the owner of Carolina Compliance Solutions (CCS), create
social media and marketing content. CCS helps general contractors keep their
subcontractors' certificates of insurance organized and on file.

Here is the approved story idea:

IDEA: {idea}
WHY IT RESONATES: {why_story}
AUDIENCE: {audience}
TOPIC: {topic}

Generate a full content set in this exact JSON format (no markdown, no extra text):
{{
  "talking_points": {{
    "hooks": ["<hook 1 — first 10 words of a TikTok/Reel opener>",
              "<hook 2>",
              "<hook 3>"],
    "bullets": ["<bullet 1 — one crisp line>",
                "<bullet 2>",
                "<bullet 3>",
                "<bullet 4>"]
  }},
  "linkedin_post": {{
    "hooks": ["<opening line option 1>", "<option 2>", "<option 3>"],
    "body": "<full post — professional tone, 150-200 words, no hashtags required>"
  }},
  "facebook_post": {{
    "hooks": ["<opening line option 1>", "<option 2>", "<option 3>"],
    "body": "<full post — warm, conversational, 100-150 words>"
  }},
  "prospect_email": {{
    "hooks": ["<subject line option 1>", "<option 2>", "<option 3>"],
    "body": "<email body — friendly, 80-120 words, ends with a low-pressure invite to chat>"
  }},
  "website_faq": {{
    "hooks": ["<question phrasing 1>", "<phrasing 2>", "<phrasing 3>"],
    "answer": "<answer — clear, 2-3 sentences, no jargon>"
  }}
}}

Hard rules (apply to every piece):
- Never use: compliant, verified, covered, certified — say "on file" or "not on file" instead
- Do not name any business, person, policy number, or carrier
- No state-specific legal claims — audience spans all states
- Do not say auditors review past years or require every policy term
- Do not say the business "relies on AI" — say it streamlines the process
- CCS website and "happy to chat" go in email and LinkedIn only (not TikTok bullets)
- Voice: plain-spoken, warm, short sentences, real and capable — not hype
- Billy/RTR content: set needs_billy_okay to true (but do not add this to JSON — handled separately)
"""


def _call_claude(idea_fields: dict) -> dict | None:
    """Call Claude Haiku with the idea fields; parse and return the JSON."""
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set")
        return None
    try:
        client = anthropic.Anthropic(api_key=api_key)
        prompt = _CONTENT_PROMPT.format(
            idea=idea_fields.get(MI_IDEA, ""),
            why_story=idea_fields.get(MI_WHY_STORY, ""),
            audience=idea_fields.get(MI_AUDIENCE, "GC"),
            topic=idea_fields.get(MI_TOPIC, ""),
        )
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=1800,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = (resp.content[0].text or "").strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as exc:
        log.warning("Claude content generation failed: %s", exc)
        return None


def _guard_text(text: str) -> tuple[bool, str]:
    """Run guard on text. Returns (ok, result_string)."""
    r = guard_check(text)
    if r.ok:
        return True, "PASS"
    return False, f"FAIL: {'; '.join(r.flags)}"


def _write_content_queue_record(
    idea_id: str,
    platform: str,
    draft_text: str,
    hooks: list[str],
    guard_result: str,
    needs_billy: bool,
) -> str | None:
    """Write one Content Queue record. Returns created record ID or None."""
    api = pyairtable.Api(os.environ["AIRTABLE_API_KEY"])
    tbl = api.table(MARKETING_BASE_ID, CONTENT_QUEUE_TABLE_ID)
    tracked_link = build_utm_link(draft_text, platform, page=UTM_PAGE_MAP.get(platform, "home"))
    fields = {
        CQ_DRAFT_TEXT:    anonymize(draft_text),
        CQ_PLATFORM:      platform,
        CQ_HOOKS:         "\n---\n".join(anonymize(h) for h in hooks),
        CQ_STATUS:        CQ_STATUS_DRAFT,
        CQ_NEEDS_BILLY:   needs_billy,
        CQ_GUARD_RESULT:  guard_result,
        CQ_TRACKED_LINK:  tracked_link,
        CQ_IDEA_LINK:     [idea_id],
    }
    try:
        rec = tbl.create(fields)
        return rec["id"]
    except Exception as exc:
        log.warning("Failed to write Content Queue record (%s): %s", platform, exc)
        return None


def generate_content_set(idea_id: str, idea_fields: dict, needs_billy: bool = False) -> dict:
    """
    Generate a full content set for one approved idea.

    Args:
        idea_id:      Airtable record ID of the Marketing Idea.
        idea_fields:  The fields dict from that record.
        needs_billy:  True if the idea involves the Billy/RTR real-client batch.

    Returns:
        Summary dict: {created, blocked, platform_results}.
    """
    result = {"created": 0, "blocked": 0, "platform_results": []}

    content = _call_claude(idea_fields)
    if not content:
        log.warning("No content generated for idea %s", idea_id)
        return result

    platform_specs = [
        ("TikTok",       content.get("talking_points", {})),
        ("Facebook",     content.get("facebook_post", {})),
        ("LinkedIn",     content.get("linkedin_post", {})),
        ("Email",        content.get("prospect_email", {})),
        ("Website FAQ",  content.get("website_faq", {})),
    ]

    for platform, piece in platform_specs:
        if not piece:
            continue

        # For talking points the main text is the bullets joined; others use "body" or "answer"
        if platform == "TikTok":
            bullets = piece.get("bullets", [])
            draft_text = "\n".join(f"• {b}" for b in bullets)
        elif platform == "Website FAQ":
            draft_text = piece.get("answer", "")
        else:
            draft_text = piece.get("body", "")

        hooks = piece.get("hooks", [])
        if not draft_text:
            continue

        # Combine draft + hooks for guard check
        combined = draft_text + " " + " ".join(hooks)
        ok, guard_result = _guard_text(combined)

        if not ok:
            log.warning("Content blocked for %s: %s", platform, guard_result)
            result["blocked"] += 1
            result["platform_results"].append({"platform": platform, "status": "blocked", "reason": guard_result})
            continue

        rec_id = _write_content_queue_record(
            idea_id=idea_id,
            platform=platform,
            draft_text=draft_text,
            hooks=hooks,
            guard_result=guard_result,
            needs_billy=needs_billy,
        )
        if rec_id:
            result["created"] += 1
            result["platform_results"].append({"platform": platform, "status": "created", "id": rec_id})
            log.info("Content Queue record created: %s (%s)", rec_id, platform)

    return result


def load_approved_ideas(base_id: str = MARKETING_BASE_ID) -> list[tuple[str, dict]]:
    """Return (record_id, fields) tuples for all Approved ideas with no content yet."""
    api = pyairtable.Api(os.environ["AIRTABLE_API_KEY"])
    ideas_tbl = api.table(base_id, "tblEUTFIA4SU7lwcu")  # Marketing Ideas
    queue_tbl = api.table(base_id, CONTENT_QUEUE_TABLE_ID)

    # IDs of ideas that already have content queue records
    try:
        existing = queue_tbl.all(fields=[CQ_IDEA_LINK])
        covered_idea_ids: set[str] = set()
        for rec in existing:
            for linked in rec.get("fields", {}).get(CQ_IDEA_LINK, []):
                covered_idea_ids.add(linked)
    except Exception:
        covered_idea_ids = set()

    try:
        approved = ideas_tbl.all(
            formula="{Status}='Approved'",
            fields=[MI_IDEA, MI_WHY_STORY, MI_AUDIENCE, MI_TOPIC],
        )
    except Exception as exc:
        log.warning("Could not load approved ideas: %s", exc)
        return []

    return [
        (rec["id"], rec["fields"])
        for rec in approved
        if rec["id"] not in covered_idea_ids
    ]
