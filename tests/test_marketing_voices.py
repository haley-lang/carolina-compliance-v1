"""Tests for marketing_content_generator.py — mocks Anthropic and Airtable."""
import sys
import os
import json
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from marketing_constants import (
    VOICES, CTAS, CHANNEL_LINKEDIN_PERSONAL, CHANNEL_CCS_TIKTOK,
    CHANNEL_LOCAL_FACEBOOK_GROUP, CHANNEL_CONTRACTOR_FACEBOOK_GROUP,
    CHANNEL_CCS_FACEBOOK, CHANNELS_NOT_SETUP, CHANNEL_STATUS_NOT_SETUP,
    VOICE_FRIENDLY_NEIGHBOR, VOICE_CALM_EXPERT, VOICE_STORYTELLER,
    CTA_MESSAGE_ME, CTA_SIGN_UP,
)
from marketing_content_generator import (
    _assemble_text_draft, _assemble_tiktok_draft,
    _GROUP_FOOTER,
)


# ── Draft assembly helpers ────────────────────────────────────────────────────

def test_assemble_text_draft_appends_cta_message_me():
    body = "Here is some content."
    link = "https://ccs.com/?utm_source=test"
    draft = _assemble_text_draft(body, CHANNEL_LINKEDIN_PERSONAL, CTA_MESSAGE_ME, link)
    assert "Have questions? Send me a message." in draft
    assert link not in draft.split("Have questions?")[0]  # CTA comes after body


def test_assemble_text_draft_appends_cta_sign_up():
    body = "Here is some content."
    link = "https://ccs.com/?utm_source=test"
    draft = _assemble_text_draft(body, CHANNEL_LINKEDIN_PERSONAL, CTA_SIGN_UP, link)
    assert "Ready to get organized?" in draft
    assert link in draft


def test_assemble_group_draft_has_reply_line():
    body = "Value-first content here."
    link = "https://ccs.com/?utm_source=test"
    draft = _assemble_text_draft(body, CHANNEL_LOCAL_FACEBOOK_GROUP, CTA_MESSAGE_ME, link)
    assert "Reply if asked:" in draft
    assert link in draft  # link is in the footer, not body


def test_assemble_group_draft_has_self_promo_note():
    body = "More content."
    link = "https://ccs.com/?utm_source=test"
    draft = _assemble_text_draft(body, CHANNEL_CONTRACTOR_FACEBOOK_GROUP, CTA_SIGN_UP, link)
    assert "self-promo rules" in draft.lower() or "Check this group" in draft


def test_assemble_tiktok_draft_has_all_sections():
    piece = {
        "hook": "Here's what I saw on a certificate review.",
        "beat_1": "The GC had no idea the sub's policy had lapsed.",
        "beat_2": "It showed up during a routine audit.",
        "beat_3": "One call fixed it. Because the system flagged it first.",
        "on_screen_text": "expired COI, found it, fixed it",
        "read_aloud": "So here's what happened. I was reviewing certificates...",
    }
    link = "https://ccs.com/?utm_source=test"
    draft = _assemble_tiktok_draft(piece, CTA_MESSAGE_ME, link)
    assert "HOOK:" in draft
    assert "BEAT 1:" in draft
    assert "BEAT 2:" in draft
    assert "BEAT 3:" in draft
    assert "ON-SCREEN TEXT:" in draft
    assert "READ ALOUD:" in draft


def test_assemble_tiktok_draft_includes_cta():
    piece = {"hook": "Test", "beat_1": "B1", "beat_2": "B2", "beat_3": "B3",
             "on_screen_text": "text", "read_aloud": "read aloud text"}
    link = "https://ccs.com/?utm_source=test"
    draft = _assemble_tiktok_draft(piece, CTA_SIGN_UP, link)
    assert "Ready to get organized?" in draft


# ── Constants / channel sets ──────────────────────────────────────────────────

def test_voices_list_has_three():
    assert len(VOICES) == 3
    assert VOICE_FRIENDLY_NEIGHBOR in VOICES
    assert VOICE_CALM_EXPERT in VOICES
    assert VOICE_STORYTELLER in VOICES


def test_ctas_list_has_two():
    assert len(CTAS) == 2
    assert CTA_MESSAGE_ME in CTAS
    assert CTA_SIGN_UP in CTAS


def test_channels_not_setup_contains_facebook_and_tiktok():
    assert CHANNEL_CCS_FACEBOOK in CHANNELS_NOT_SETUP
    assert CHANNEL_CCS_TIKTOK in CHANNELS_NOT_SETUP
    assert CHANNEL_LINKEDIN_PERSONAL not in CHANNELS_NOT_SETUP


# ── Full generate_content_set integration (mocked) ───────────────────────────

def _make_text_content():
    voice_block = {v: {"body": f"Draft for voice {v}.", "hooks": ["Hook A", "Hook B", "Hook C"]}
                   for v in VOICES}
    return {
        CHANNEL_LINKEDIN_PERSONAL: voice_block,
        CHANNEL_CCS_FACEBOOK: voice_block,
        CHANNEL_LOCAL_FACEBOOK_GROUP: voice_block,
        CHANNEL_CONTRACTOR_FACEBOOK_GROUP: voice_block,
    }


def _make_tiktok_content():
    return {v: {
        "hook": "Hook text",
        "beat_1": "Beat one",
        "beat_2": "Beat two",
        "beat_3": "Beat three",
        "on_screen_text": "word1, word2",
        "read_aloud": "Read aloud version",
    } for v in VOICES}


def test_generate_content_set_creates_records(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AIRTABLE_API_KEY", "test-key")

    created_records = []

    def fake_tbl_create(fields):
        rec_id = f"rec{len(created_records):03d}"
        created_records.append(fields)
        return {"id": rec_id}

    mock_tbl = MagicMock()
    mock_tbl.create.side_effect = fake_tbl_create

    mock_api = MagicMock()
    mock_api.table.return_value = mock_tbl

    with patch("marketing_content_generator._build_text_content", return_value=_make_text_content()):
        with patch("marketing_content_generator._build_tiktok_content", return_value=_make_tiktok_content()):
            with patch("marketing_content_generator.check_claims", return_value=[]):
                with patch("pyairtable.Api", return_value=mock_api):
                    with patch("site_knowledge.get_all_pages", return_value=[]):
                        from marketing_content_generator import generate_content_set
                        result = generate_content_set("recIdea001", {
                            "Idea": "Test idea",
                            "Why It's a Story": "It resonates",
                            "Audience": "GC",
                            "Topic": "Certificates",
                        }, channel=None)

    # 4 text channels × 3 voices × 2 CTAs + 1 TikTok channel × 3 voices × 2 CTAs = 30
    assert result["created"] == 30
    assert result["blocked"] == 0


def test_generate_content_set_records_claim_flags(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AIRTABLE_API_KEY", "test-key")

    written_fields = []

    def fake_create(fields):
        written_fields.append(fields)
        return {"id": f"rec{len(written_fields):03d}"}

    mock_tbl = MagicMock()
    mock_tbl.create.side_effect = fake_create
    mock_api = MagicMock()
    mock_api.table.return_value = mock_tbl

    with patch("marketing_content_generator._build_text_content", return_value=_make_text_content()):
        with patch("marketing_content_generator._build_tiktok_content", return_value={}):
            with patch("marketing_content_generator.check_claims", return_value=["unsupported claim"]):
                with patch("pyairtable.Api", return_value=mock_api):
                    with patch("site_knowledge.get_all_pages", return_value=[{"title":"T","body_text":"B","url":"U"}]):
                        from marketing_content_generator import generate_content_set
                        generate_content_set("recIdea002", {
                            "Idea": "Test",
                            "Why It's a Story": "Resonates",
                            "Audience": "GC",
                            "Topic": "Certs",
                        })

    # Every record should have a Claim Flags value
    from marketing_constants import CQ_CLAIM_FLAGS
    for fields in written_fields:
        assert fields.get(CQ_CLAIM_FLAGS) == "unsupported claim"


def test_channel_status_set_for_unsetup_channels(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AIRTABLE_API_KEY", "test-key")

    written_fields = []

    def fake_create(fields):
        written_fields.append(fields)
        return {"id": f"rec{len(written_fields):03d}"}

    mock_tbl = MagicMock()
    mock_tbl.create.side_effect = fake_create
    mock_api = MagicMock()
    mock_api.table.return_value = mock_tbl

    with patch("marketing_content_generator._build_text_content", return_value=_make_text_content()):
        with patch("marketing_content_generator._build_tiktok_content", return_value=_make_tiktok_content()):
            with patch("marketing_content_generator.check_claims", return_value=[]):
                with patch("pyairtable.Api", return_value=mock_api):
                    with patch("site_knowledge.get_all_pages", return_value=[]):
                        from marketing_content_generator import generate_content_set
                        generate_content_set("recIdea003", {
                            "Idea": "Test",
                            "Why It's a Story": "Resonates",
                            "Audience": "GC",
                            "Topic": "Certs",
                        })

    from marketing_constants import CQ_CHANNEL, CQ_CHANNEL_STATUS
    for fields in written_fields:
        channel = fields.get(CQ_CHANNEL, "")
        if channel in CHANNELS_NOT_SETUP:
            assert fields.get(CQ_CHANNEL_STATUS) == CHANNEL_STATUS_NOT_SETUP
        else:
            assert not fields.get(CQ_CHANNEL_STATUS)


# ── Single-channel mode ───────────────────────────────────────────────────────

def _make_generate_patches(text_content, tiktok_content, mock_api):
    return [
        patch("marketing_content_generator._build_text_content", return_value=text_content),
        patch("marketing_content_generator._build_tiktok_content", return_value=tiktok_content),
        patch("marketing_content_generator.check_claims", return_value=[]),
        patch("pyairtable.Api", return_value=mock_api),
        patch("site_knowledge.get_all_pages", return_value=[]),
    ]


def _run_generate(idea_id, patches, channel):
    from contextlib import ExitStack
    from marketing_content_generator import generate_content_set
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        return generate_content_set(idea_id, {
            "Idea": "Test idea", "Why It's a Story": "Resonates",
            "Audience": "GC", "Topic": "Certs",
        }, channel=channel)


def test_single_channel_generates_three_drafts(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AIRTABLE_API_KEY", "test-key")

    created = []
    mock_tbl = MagicMock()
    mock_tbl.create.side_effect = lambda f: (created.append(f), {"id": f"rec{len(created):03d}"})[1]
    mock_api = MagicMock()
    mock_api.table.return_value = mock_tbl

    patches = _make_generate_patches(_make_text_content(), _make_tiktok_content(), mock_api)
    result = _run_generate("recIdea004", patches, channel=CHANNEL_LINKEDIN_PERSONAL)

    assert result["created"] == 3
    assert result["blocked"] == 0


def test_cta_alternates_across_voices(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AIRTABLE_API_KEY", "test-key")

    written = []
    mock_tbl = MagicMock()
    mock_tbl.create.side_effect = lambda f: (written.append(f), {"id": f"rec{len(written):03d}"})[1]
    mock_api = MagicMock()
    mock_api.table.return_value = mock_tbl

    patches = _make_generate_patches(_make_text_content(), {}, mock_api)
    _run_generate("recIdea005", patches, channel=CHANNEL_LINKEDIN_PERSONAL)

    from marketing_constants import CQ_CTA_TYPE, CQ_VOICE
    ctas_by_voice = {f[CQ_VOICE]: f[CQ_CTA_TYPE] for f in written}
    # voice_idx 0 → CTAS[0]=message_me, 1 → CTAS[1]=sign_up, 2 → CTAS[0]=message_me
    assert ctas_by_voice[VOICE_FRIENDLY_NEIGHBOR] == CTA_MESSAGE_ME
    assert ctas_by_voice[VOICE_CALM_EXPERT] == CTA_SIGN_UP
    assert ctas_by_voice[VOICE_STORYTELLER] == CTA_MESSAGE_ME


def test_tiktok_single_channel_generates_three_drafts(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AIRTABLE_API_KEY", "test-key")

    created = []
    mock_tbl = MagicMock()
    mock_tbl.create.side_effect = lambda f: (created.append(f), {"id": f"rec{len(created):03d}"})[1]
    mock_api = MagicMock()
    mock_api.table.return_value = mock_tbl

    patches = _make_generate_patches({}, _make_tiktok_content(), mock_api)
    result = _run_generate("recIdea006", patches, channel=CHANNEL_CCS_TIKTOK)

    assert result["created"] == 3


def test_unknown_channel_returns_empty(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AIRTABLE_API_KEY", "test-key")

    created = []
    mock_tbl = MagicMock()
    mock_tbl.create.side_effect = lambda f: (created.append(f), {"id": f"rec{len(created):03d}"})[1]
    mock_api = MagicMock()
    mock_api.table.return_value = mock_tbl

    patches = _make_generate_patches(_make_text_content(), _make_tiktok_content(), mock_api)
    result = _run_generate("recIdea007", patches, channel="bad_channel")

    assert result["created"] == 0
