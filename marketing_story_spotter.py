"""
Marketing story-spotter cron job.
Follows the pulse_cron.py pattern: runs on a Railway cron schedule.

Workflow:
  1. Collect new signals via marketing_signals.py
  2. Write them to Marketing Signals table
  3. For each signal, ask Claude Haiku to generate an anonymized story idea
  4. Run every idea through compliance_guard before saving
  5. Write passing ideas to Marketing Ideas table (status = New)

Railway cron schedule (add to railway.toml):
  [cron.marketing_story_spotter]
  schedule = "0 14 * * 1,3,5"   # Mon/Wed/Fri at 14:00 UTC
  command  = "python marketing_story_spotter.py"
"""
from __future__ import annotations
import logging
import os
import sys
from datetime import datetime, timezone

import anthropic
import pyairtable

from marketing_constants import (
    MARKETING_BASE_ID,
    MARKETING_SIGNALS_TABLE_ID, MS_SUMMARY, MS_SOURCE, MS_RAW_REF_ID,
    MARKETING_IDEAS_TABLE_ID, MI_IDEA, MI_WHY_STORY, MI_AUDIENCE, MI_TOPIC,
    MI_STATUS, MI_SIGNAL_LINK, MI_STATUS_NEW,
)
from marketing_signals import collect_new_signals, write_signals
from compliance_guard import check as guard_check
from anonymizer import anonymize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

HAIKU_MODEL = "claude-haiku-4-5-20251001"

_IDEA_PROMPT = """\
You help a small insurance-compliance company turn real work events into content ideas.
The company is called Carolina Compliance Solutions (CCS). It helps general contractors
keep their subcontractor certificates of insurance organized and on file.

Here is an anonymized signal from recent work:

SOURCE: {source}
EVENT: {summary}

Write ONE content idea that would resonate with a general contractor or insurance agent
audience. Use plain-spoken, warm, professional language. Short sentences.

Respond in this exact JSON format (no other text):
{{
  "idea": "<one or two sentence description of the content piece>",
  "why_story": "<why this event would resonate with the audience — one sentence>",
  "audience": "<GC or Insurance agent>",
  "topic": "<one of: Audits, GL, Workers' comp, Certificate mistakes, Behind the build, DCLI experience>"
}}

Hard rules:
- Never use the words: compliant, verified, covered, certified
- Use "on file" / "not on file" instead
- Do not name any specific business, person, policy number, or carrier
- Do not make state-specific legal claims
- Do not say auditors review past years or require every policy term
- Audience is all states, keep rules general
- The company helps streamline the process; do not say it relies on AI
"""


def _call_claude(source: str, summary: str) -> dict | None:
    """Ask Claude Haiku to generate a story idea from a signal. Returns parsed dict or None."""
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set")
        return None
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=400,
            messages=[{
                "role": "user",
                "content": _IDEA_PROMPT.format(source=source, summary=summary),
            }],
        )
        raw = (resp.content[0].text or "").strip()
        import json
        # Strip markdown fences if Claude wraps the JSON
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as exc:
        log.warning("Claude idea generation failed: %s", exc)
        return None


def _guard_idea(idea_dict: dict) -> tuple[bool, list[str]]:
    """Run compliance guard over all text fields in the idea. Returns (ok, flags)."""
    combined = " ".join(str(v) for v in idea_dict.values() if isinstance(v, str))
    result = guard_check(combined)
    return result.ok, result.flags


def _write_idea(signal_id: str, idea_dict: dict, guard_flags: list[str]) -> str | None:
    """Write an idea to Marketing Ideas. Returns created record ID."""
    api = pyairtable.Api(os.environ["AIRTABLE_API_KEY"])
    tbl = api.table(MARKETING_BASE_ID, MARKETING_IDEAS_TABLE_ID)
    fields = {
        MI_IDEA: anonymize(idea_dict.get("idea", "")),
        MI_WHY_STORY: anonymize(idea_dict.get("why_story", "")),
        MI_AUDIENCE: idea_dict.get("audience", "GC"),
        MI_TOPIC: idea_dict.get("topic", "Certificate mistakes"),
        MI_STATUS: MI_STATUS_NEW,
        MI_SIGNAL_LINK: [signal_id],
    }
    # Normalize audience / topic to valid select choices
    valid_audiences = {"GC", "Insurance agent"}
    valid_topics = {"Audits", "GL", "Workers' comp", "Certificate mistakes",
                    "Behind the build", "DCLI experience"}
    if fields[MI_AUDIENCE] not in valid_audiences:
        fields[MI_AUDIENCE] = "GC"
    if fields[MI_TOPIC] not in valid_topics:
        fields[MI_TOPIC] = "Certificate mistakes"

    try:
        rec = tbl.create(fields)
        return rec["id"]
    except Exception as exc:
        log.warning("Failed to write idea: %s", exc)
        return None


def run() -> dict:
    """
    Main entry point.
    Returns summary dict: {signals_written, ideas_generated, ideas_blocked}.
    """
    log.info("Marketing story spotter starting")
    summary = {"signals_written": 0, "ideas_generated": 0, "ideas_blocked": 0}

    # Step 1: collect and write new signals
    fresh_signals = collect_new_signals()
    if not fresh_signals:
        log.info("No new signals. Done.")
        return summary

    signal_ids = write_signals(fresh_signals)
    summary["signals_written"] = len(signal_ids)
    log.info("Wrote %d signals", len(signal_ids))

    # Step 2: generate ideas
    for sig, sig_id in zip(fresh_signals, signal_ids):
        source = sig.get(MS_SOURCE, "")
        event_summary = sig.get(MS_SUMMARY, "")
        idea_dict = _call_claude(source, event_summary)
        if not idea_dict:
            continue

        ok, flags = _guard_idea(idea_dict)
        if not ok:
            log.warning("Idea blocked by compliance guard. Flags: %s", flags)
            summary["ideas_blocked"] += 1
            continue

        idea_id = _write_idea(sig_id, idea_dict, flags)
        if idea_id:
            summary["ideas_generated"] += 1
            log.info("Created idea %s", idea_id)

    log.info("Story spotter done. %s", summary)
    return summary


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    result = run()
    sys.exit(0 if result.get("signals_written", 0) >= 0 else 1)
