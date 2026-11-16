"""
Marketing content generator.
Generates drafts for one approved idea.

Default (channel specified): 1 channel × 3 voices, CTAs alternated by voice index = 3 records.
All-channels (channel=None): 5 channels × 3 voices × 2 CTAs = 30 records.

Two Claude calls for text channels (4 channels × 3 voices) and TikTok (3 voices).
When a single channel is requested we still call the same prompts and filter — token cost
is negligible for Haiku.
"""
from __future__ import annotations
import json
import logging
import os
from datetime import datetime, timezone

import anthropic
import pyairtable

from anonymizer import anonymize
from compliance_guard import check as guard_check
from marketing_claim_checker import check_claims
from marketing_constants import (
    MARKETING_BASE_ID,
    CONTENT_QUEUE_TABLE_ID,
    CQ_DRAFT_TEXT, CQ_PLATFORM, CQ_HOOKS, CQ_STATUS, CQ_NEEDS_BILLY,
    CQ_GUARD_RESULT, CQ_TRACKED_LINK, CQ_IDEA_LINK,
    CQ_VOICE, CQ_CTA_TYPE, CQ_CLAIM_FLAGS, CQ_CHANNEL, CQ_CHANNEL_STATUS,
    CQ_STATUS_DRAFT,
    MI_IDEA, MI_WHY_STORY, MI_AUDIENCE, MI_TOPIC,
    VOICES, CTAS,
    VOICE_FRIENDLY_NEIGHBOR, VOICE_CALM_EXPERT, VOICE_STORYTELLER,
    CTA_MESSAGE_ME, CTA_SIGN_UP,
    CHANNEL_LINKEDIN_PERSONAL, CHANNEL_CCS_FACEBOOK, CHANNEL_CCS_TIKTOK,
    CHANNEL_LOCAL_FACEBOOK_GROUP, CHANNEL_CONTRACTOR_FACEBOOK_GROUP,
    CHANNELS_NOT_SETUP, CHANNEL_STATUS_NOT_SETUP,
)
from utm_builder import build_utm_link

log = logging.getLogger(__name__)

HAIKU_MODEL = "claude-haiku-4-5-20251001"

_VOICE_DESCRIPTIONS = {
    VOICE_FRIENDLY_NEIGHBOR: (
        "Friendly neighbor who knows insurance. Warm, plain-spoken, practical. "
        "Talk like you're sharing useful info at a backyard cookout. Short sentences. "
        "No jargon. First-person but humble."
    ),
    VOICE_CALM_EXPERT: (
        "Calm subject-matter expert. Clear, measured, authoritative but never preachy. "
        "No unnecessary words. Lead with the fact, then what it means. "
        "Confident but never condescending."
    ),
    VOICE_STORYTELLER: (
        "Open with a real, specific moment from a certificate review — something you actually saw. "
        "First sentence lands a concrete detail. Then show what it meant. "
        "Ground every point in a real example before making the broader point."
    ),
}

_CTA_SUFFIXES = {
    CTA_MESSAGE_ME: "Have questions? Send me a message.",
    CTA_SIGN_UP:    "Ready to get organized? Sign up at [LINK].",
}

_GROUP_FOOTER = "\n\nReply if asked: [LINK]\n\n⚠️ Check this group's self-promo rules before posting."

_GROUP_NOTE = "Important: This is a Facebook group post. No link in the post body — the link goes in the 'Reply if asked' line below the post."


# ── Text channels prompt ──────────────────────────────────────────────────────

_TEXT_PROMPT = """\
You are helping Haley, the owner of Carolina Compliance Solutions (CCS), create
social media content. CCS helps general contractors keep their subcontractors'
certificates of insurance organized and on file.

IDEA: {idea}
WHY IT RESONATES: {why_story}
AUDIENCE: {audience}
TOPIC: {topic}

{kb_note}

Write drafts for 4 channels, each in 3 voices. Return a single JSON object — no markdown, no extra text.

Voice descriptions:
  friendly_neighbor: {voice_fn}
  calm_expert:       {voice_ce}
  storyteller:       {voice_st}

Channel formats:
  linkedin_personal:
    - Text post for GCs and insurance agents on personal LinkedIn
    - 150-200 words, professional but warm, no hashtags required
    - End with one clear sentence that invites a reply or conversation

  ccs_facebook:
    - CCS Facebook page post (business page, public)
    - 100-150 words, warm and conversational
    - End with an inviting sentence

  local_facebook_group:
    - Post for a local town Facebook group
    - Value-first — the whole post must be useful before any mention of CCS
    - NO link in the post body (link goes in a separate "Reply if asked" section)
    - 80-120 words
    - End sentence sets up the reply: "If you want a simple system for this..."

  contractor_facebook_group:
    - Post for a contractor/trade-focused Facebook group
    - Peer-to-peer tone, practical, specific
    - NO link in the post body
    - 80-120 words

Each channel gets each of the 3 voice variants and a list of 3 hook options (opening lines).

Hard rules (every piece):
- Never say: compliant, verified, covered, certified — say "on file" or "not on file" instead
- Never name any business, person, policy number, or carrier
- Do not make state-specific legal claims
- Do not say the product "relies on AI" — say it "streamlines the process"
- Anonymous: use "a general contractor I work with" not any real name

JSON format:
{{
  "linkedin_personal": {{
    "friendly_neighbor": {{"body": "...", "hooks": ["...", "...", "..."]}},
    "calm_expert":       {{"body": "...", "hooks": ["...", "...", "..."]}},
    "storyteller":       {{"body": "...", "hooks": ["...", "...", "..."]}}
  }},
  "ccs_facebook": {{...}},
  "local_facebook_group": {{...}},
  "contractor_facebook_group": {{...}}
}}
"""

_TIKTOK_PROMPT = """\
You are helping Haley, the owner of Carolina Compliance Solutions (CCS), write
TikTok scripts. CCS helps general contractors keep their subcontractors'
certificates of insurance organized and on file.

IDEA: {idea}
WHY IT RESONATES: {why_story}
AUDIENCE: {audience}
TOPIC: {topic}

{kb_note}

Write TikTok scripts (30-45 seconds when read aloud) in 3 voices.
Return a single JSON object — no markdown, no extra text.

Voice descriptions:
  friendly_neighbor: {voice_fn}
  calm_expert:       {voice_ce}
  storyteller:       {voice_st}

Each script has these sections:
  hook:          First line. Attention-grabbing, conversational. Under 10 words.
  beat_1:        First main point. 1-2 sentences.
  beat_2:        Second main point. 1-2 sentences.
  beat_3:        Third main point or payoff. 1-2 sentences.
  on_screen_text: 3-4 word phrases for text overlay (comma-separated string)
  read_aloud:    Casual spoken version — the full script read naturally out loud, 30-45 sec

Hard rules:
- Never say: compliant, verified, covered, certified — say "on file" or "not on file" instead
- Never name any business, person, policy number, or carrier
- No state-specific legal claims
- Use "a general contractor I work with" not any real name
- Casual, real, no buzzwords

JSON format:
{{
  "friendly_neighbor": {{
    "hook": "...", "beat_1": "...", "beat_2": "...", "beat_3": "...",
    "on_screen_text": "...", "read_aloud": "..."
  }},
  "calm_expert": {{...}},
  "storyteller": {{...}}
}}
"""


# ── Claude calls ──────────────────────────────────────────────────────────────

def _kb_note(kb_pages: list[dict]) -> str:
    if not kb_pages:
        return ""
    titles = [p["title"] for p in kb_pages if p.get("title")][:8]
    return (
        "Reference: only make claims that are supported by the CCS website. "
        f"The site covers: {', '.join(titles)}. "
        "If the idea would require a claim not on the site, write around it."
    )


def _call_claude(prompt: str, max_tokens: int = 4096) -> dict | None:
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set")
        return None
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = (resp.content[0].text or "").strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as exc:
        log.warning("Claude call failed: %s", exc)
        return None


def _build_text_content(idea_fields: dict, kb_pages: list[dict]) -> dict | None:
    prompt = _TEXT_PROMPT.format(
        idea=idea_fields.get(MI_IDEA, ""),
        why_story=idea_fields.get(MI_WHY_STORY, ""),
        audience=idea_fields.get(MI_AUDIENCE, "GC"),
        topic=idea_fields.get(MI_TOPIC, ""),
        kb_note=_kb_note(kb_pages),
        voice_fn=_VOICE_DESCRIPTIONS[VOICE_FRIENDLY_NEIGHBOR],
        voice_ce=_VOICE_DESCRIPTIONS[VOICE_CALM_EXPERT],
        voice_st=_VOICE_DESCRIPTIONS[VOICE_STORYTELLER],
    )
    return _call_claude(prompt, max_tokens=4096)


def _build_tiktok_content(idea_fields: dict, kb_pages: list[dict]) -> dict | None:
    prompt = _TIKTOK_PROMPT.format(
        idea=idea_fields.get(MI_IDEA, ""),
        why_story=idea_fields.get(MI_WHY_STORY, ""),
        audience=idea_fields.get(MI_AUDIENCE, "GC"),
        topic=idea_fields.get(MI_TOPIC, ""),
        kb_note=_kb_note(kb_pages),
        voice_fn=_VOICE_DESCRIPTIONS[VOICE_FRIENDLY_NEIGHBOR],
        voice_ce=_VOICE_DESCRIPTIONS[VOICE_CALM_EXPERT],
        voice_st=_VOICE_DESCRIPTIONS[VOICE_STORYTELLER],
    )
    return _call_claude(prompt, max_tokens=2048)


# ── Draft assembly ────────────────────────────────────────────────────────────

def _guard_text(text: str) -> tuple[bool, str]:
    r = guard_check(text)
    if r.ok:
        return True, "PASS"
    return False, f"FAIL: {'; '.join(r.flags)}"


def _assemble_text_draft(body: str, channel: str, cta_type: str, link: str) -> str:
    """Add CTA suffix and group footer as appropriate."""
    is_group = channel in (CHANNEL_LOCAL_FACEBOOK_GROUP, CHANNEL_CONTRACTOR_FACEBOOK_GROUP)
    cta_suffix = _CTA_SUFFIXES[cta_type].replace("[LINK]", link)

    if is_group:
        footer = _GROUP_FOOTER.replace("[LINK]", link)
        return f"{body}\n\n{cta_suffix}{footer}"
    return f"{body}\n\n{cta_suffix}"


def _assemble_tiktok_draft(piece: dict, cta_type: str, link: str) -> str:
    """Format a TikTok script from the piece dict."""
    cta_suffix = _CTA_SUFFIXES[cta_type].replace("[LINK]", link)
    lines = [
        f"HOOK: {piece.get('hook', '')}",
        f"\nBEAT 1: {piece.get('beat_1', '')}",
        f"BEAT 2: {piece.get('beat_2', '')}",
        f"BEAT 3: {piece.get('beat_3', '')}",
        f"\nON-SCREEN TEXT: {piece.get('on_screen_text', '')}",
        f"\nREAD ALOUD:\n{piece.get('read_aloud', '')}",
        f"\n{cta_suffix}",
    ]
    return "\n".join(lines)


# ── Airtable write ────────────────────────────────────────────────────────────

def _write_record(
    tbl,
    idea_id: str,
    channel: str,
    voice: str,
    cta_type: str,
    draft_text: str,
    hooks: list[str],
    guard_result: str,
    claim_flags: list[str],
    needs_billy: bool,
) -> str | None:
    link = build_utm_link(draft_text, channel, page="home", cta_type=cta_type)
    channel_status = CHANNEL_STATUS_NOT_SETUP if channel in CHANNELS_NOT_SETUP else ""
    fields = {
        CQ_DRAFT_TEXT:    anonymize(draft_text),
        CQ_PLATFORM:      channel,
        CQ_CHANNEL:       channel,
        CQ_VOICE:         voice,
        CQ_CTA_TYPE:      cta_type,
        CQ_HOOKS:         "\n---\n".join(anonymize(h) for h in hooks),
        CQ_STATUS:        CQ_STATUS_DRAFT,
        CQ_NEEDS_BILLY:   needs_billy,
        CQ_GUARD_RESULT:  guard_result,
        CQ_TRACKED_LINK:  link,
        CQ_IDEA_LINK:     [idea_id],
        CQ_CLAIM_FLAGS:   "\n".join(claim_flags) if claim_flags else "",
    }
    if channel_status:
        fields[CQ_CHANNEL_STATUS] = channel_status
    try:
        rec = tbl.create(fields)
        return rec["id"]
    except Exception as exc:
        log.warning("write_record failed (%s/%s/%s): %s", channel, voice, cta_type, exc)
        return None


# ── Main entry point ──────────────────────────────────────────────────────────

_TEXT_CHANNELS = [
    CHANNEL_LINKEDIN_PERSONAL,
    CHANNEL_CCS_FACEBOOK,
    CHANNEL_LOCAL_FACEBOOK_GROUP,
    CHANNEL_CONTRACTOR_FACEBOOK_GROUP,
]


def generate_content_set(
    idea_id: str,
    idea_fields: dict,
    needs_billy: bool = False,
    channel: str | None = None,
) -> dict:
    """
    Generate drafts for one approved idea.

    channel=None  — all 5 channels × 3 voices × 2 CTAs = 30 records.
    channel=<name> — 1 channel × 3 voices, CTAs alternated by voice index = 3 records.

    Returns: {created, blocked, platform_results}.
    """
    result: dict = {"created": 0, "blocked": 0, "platform_results": []}

    all_channels_mode = channel is None

    # Load KB pages (best-effort — generation still runs if KB unavailable)
    kb_pages: list[dict] = []
    try:
        import pyairtable as _pat
        api = _pat.Api(os.environ["AIRTABLE_API_KEY"])
        from site_knowledge import get_all_pages
        kb_pages = get_all_pages(api)
        tbl = api.table(MARKETING_BASE_ID, CONTENT_QUEUE_TABLE_ID)
    except Exception as exc:
        log.warning("KB load failed, continuing without claim check: %s", exc)
        api = pyairtable.Api(os.environ["AIRTABLE_API_KEY"])
        tbl = api.table(MARKETING_BASE_ID, CONTENT_QUEUE_TABLE_ID)

    # Determine which text channels and whether to do TikTok
    if all_channels_mode:
        target_text_channels = _TEXT_CHANNELS
        do_tiktok = True
    elif channel == CHANNEL_CCS_TIKTOK:
        target_text_channels = []
        do_tiktok = True
    elif channel in _TEXT_CHANNELS:
        target_text_channels = [channel]
        do_tiktok = False
    else:
        log.warning("Unknown channel %r — generating nothing", channel)
        return result

    # ── Text channels ─────────────────────────────────────────────────────────
    if target_text_channels:
        text_content = _build_text_content(idea_fields, kb_pages)
        if text_content:
            for ch in target_text_channels:
                channel_data = text_content.get(ch, {})
                if not channel_data:
                    log.warning("No text content for channel %s", ch)
                    continue
                for voice_idx, voice in enumerate(VOICES):
                    piece = channel_data.get(voice, {})
                    if not piece:
                        continue
                    body = piece.get("body", "")
                    hooks = piece.get("hooks", [])
                    if not body:
                        continue

                    # All-channels: full matrix. Single-channel: alternate CTA by voice index.
                    cta_list = CTAS if all_channels_mode else [CTAS[voice_idx % len(CTAS)]]

                    for cta_type in cta_list:
                        link = build_utm_link(body, ch, page="home", cta_type=cta_type)
                        draft_text = _assemble_text_draft(body, ch, cta_type, link)
                        draft_text = anonymize(draft_text)

                        combined = draft_text + " " + " ".join(hooks)
                        ok, guard_result = _guard_text(combined)
                        if not ok:
                            log.warning("Blocked %s/%s/%s: %s", ch, voice, cta_type, guard_result)
                            result["blocked"] += 1
                            result["platform_results"].append({
                                "channel": ch, "voice": voice, "cta": cta_type,
                                "status": "blocked", "reason": guard_result,
                            })
                            continue

                        claim_flags = check_claims(draft_text, kb_pages)

                        rec_id = _write_record(
                            tbl=tbl, idea_id=idea_id,
                            channel=ch, voice=voice, cta_type=cta_type,
                            draft_text=draft_text, hooks=hooks,
                            guard_result=guard_result, claim_flags=claim_flags,
                            needs_billy=needs_billy,
                        )
                        if rec_id:
                            result["created"] += 1
                            result["platform_results"].append({
                                "channel": ch, "voice": voice, "cta": cta_type,
                                "status": "created", "id": rec_id,
                                "claim_flags": claim_flags,
                            })

    # ── TikTok ────────────────────────────────────────────────────────────────
    if do_tiktok:
        tiktok_content = _build_tiktok_content(idea_fields, kb_pages)
        if tiktok_content:
            for voice_idx, voice in enumerate(VOICES):
                piece = tiktok_content.get(voice, {})
                if not piece:
                    continue

                cta_list = CTAS if all_channels_mode else [CTAS[voice_idx % len(CTAS)]]

                for cta_type in cta_list:
                    link = build_utm_link(
                        idea_fields.get(MI_IDEA, ""),
                        CHANNEL_CCS_TIKTOK,
                        page="home",
                        cta_type=cta_type,
                    )
                    draft_text = _assemble_tiktok_draft(piece, cta_type, link)
                    draft_text = anonymize(draft_text)
                    hooks = [piece.get("hook", "")]

                    combined = draft_text + " " + piece.get("hook", "")
                    ok, guard_result = _guard_text(combined)
                    if not ok:
                        result["blocked"] += 1
                        result["platform_results"].append({
                            "channel": CHANNEL_CCS_TIKTOK, "voice": voice, "cta": cta_type,
                            "status": "blocked", "reason": guard_result,
                        })
                        continue

                    claim_flags = check_claims(draft_text, kb_pages)

                    rec_id = _write_record(
                        tbl=tbl, idea_id=idea_id,
                        channel=CHANNEL_CCS_TIKTOK, voice=voice, cta_type=cta_type,
                        draft_text=draft_text, hooks=hooks,
                        guard_result=guard_result, claim_flags=claim_flags,
                        needs_billy=needs_billy,
                    )
                    if rec_id:
                        result["created"] += 1
                        result["platform_results"].append({
                            "channel": CHANNEL_CCS_TIKTOK, "voice": voice, "cta": cta_type,
                            "status": "created", "id": rec_id,
                            "claim_flags": claim_flags,
                        })

    return result


def load_approved_ideas(base_id: str = MARKETING_BASE_ID) -> list[tuple[str, dict]]:
    """Return (record_id, fields) tuples for Approved ideas with no content yet."""
    api = pyairtable.Api(os.environ["AIRTABLE_API_KEY"])
    ideas_tbl = api.table(base_id, "tblEUTFIA4SU7lwcu")
    queue_tbl = api.table(base_id, CONTENT_QUEUE_TABLE_ID)

    try:
        existing = queue_tbl.all(fields=[CQ_IDEA_LINK])
        covered: set[str] = set()
        for rec in existing:
            for linked in rec.get("fields", {}).get(CQ_IDEA_LINK, []):
                covered.add(linked)
    except Exception:
        covered = set()

    try:
        from marketing_constants import MI_STATUS, MI_STATUS_APPROVED
        approved = ideas_tbl.all(
            formula=f"{{{MI_STATUS}}}='{MI_STATUS_APPROVED}'",
            fields=[MI_IDEA, MI_WHY_STORY, MI_AUDIENCE, MI_TOPIC],
        )
    except Exception as exc:
        log.warning("load_approved_ideas failed: %s", exc)
        return []

    return [(rec["id"], rec["fields"]) for rec in approved if rec["id"] not in covered]
