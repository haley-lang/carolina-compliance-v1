"""
Marketing claim checker.
Uses Claude Haiku to identify factual claims in a draft that are not
supported by the CCS website knowledge base.

Returns a list of flag strings — empty list means no unsupported claims found.
"""
from __future__ import annotations
import json
import logging
import os

import anthropic

log = logging.getLogger(__name__)

HAIKU_MODEL = "claude-haiku-4-5-20251001"
MAX_KB_CHARS = 6000  # max KB context we send to Claude


def _build_kb_context(kb_pages: list[dict]) -> str:
    """Flatten KB pages into a compact context string."""
    chunks: list[str] = []
    total = 0
    for page in kb_pages:
        title = page.get("title", "")
        body = page.get("body_text", "")
        snippet = f"[{title}]\n{body}"
        if total + len(snippet) > MAX_KB_CHARS:
            remaining = MAX_KB_CHARS - total
            if remaining > 100:
                chunks.append(snippet[:remaining])
            break
        chunks.append(snippet)
        total += len(snippet)
    return "\n\n".join(chunks)


_CHECK_PROMPT = """\
You are a marketing compliance reviewer for Carolina Compliance Solutions (CCS).

Here is the content from the CCS public website:

<website_content>
{kb_context}
</website_content>

Here is a marketing draft to review:

<draft>
{draft_text}
</draft>

Identify any FACTUAL CLAIMS in the draft that are NOT supported by the website content above.
Only flag specific, verifiable claims — not general value statements like "we can help you" or
"stay organized". Focus on specific numbers, features, processes, or service promises.

Respond with a JSON array of flagged claim strings. If nothing needs flagging, return an empty array [].
No other text — only the JSON array.
"""


def check_claims(draft_text: str, kb_pages: list[dict]) -> list[str]:
    """
    Check draft_text against kb_pages for unsupported factual claims.
    Returns list of flagged claim strings (empty = all clear).
    """
    if not kb_pages or not draft_text.strip():
        return []

    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        log.warning("claim_checker: ANTHROPIC_API_KEY not set, skipping check")
        return []

    kb_context = _build_kb_context(kb_pages)
    prompt = _CHECK_PROMPT.format(
        kb_context=kb_context,
        draft_text=draft_text[:3000],
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = (resp.content[0].text or "").strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        flags = json.loads(raw)
        if isinstance(flags, list):
            return [str(f) for f in flags if f]
    except Exception as exc:
        log.warning("claim_checker: failed: %s", exc)

    return []
