"""Tests for Stage 2: marketing content generator and /generate route."""
import os
import time
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("REVIEW_PASSWORD", "test_review_pass")
os.environ.setdefault("ANTHROPIC_API_KEY", "test_key")
os.environ.setdefault("AIRTABLE_API_KEY", "test_airtable_key")


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def app():
    import review_app
    review_app.app.config["TESTING"] = True
    return review_app.app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def authed_client(client):
    with client.session_transaction() as sess:
        sess["authenticated"] = True
        sess["logged_in_at"] = time.time()
        sess["csrf_token"] = "test_csrf_32chars_paddingpadding0"
    return client


_SAMPLE_CONTENT = {
    "talking_points": {
        "hooks": ["Hook 1 for TikTok", "Hook 2 for TikTok", "Hook 3 for TikTok"],
        "bullets": ["Bullet one", "Bullet two", "Bullet three", "Bullet four"],
    },
    "linkedin_post": {
        "hooks": ["LinkedIn hook 1", "LinkedIn hook 2", "LinkedIn hook 3"],
        "body": "This is the LinkedIn post body text, written professionally.",
    },
    "facebook_post": {
        "hooks": ["Facebook hook 1", "Facebook hook 2", "Facebook hook 3"],
        "body": "This is a warm, conversational Facebook post body.",
    },
    "prospect_email": {
        "hooks": ["Subject line 1", "Subject line 2", "Subject line 3"],
        "body": "Hi there! Just reaching out. Happy to chat.",
    },
    "website_faq": {
        "hooks": ["FAQ question phrasing 1", "Phrasing 2", "Phrasing 3"],
        "answer": "CCS helps keep certificates on file. Two clear sentences.",
    },
}

_IDEA_FIELDS = {
    "Idea": "Show how having everything on file saved audit day.",
    "Why It's a Story": "GCs relate to audit prep stress.",
    "Audience": "GC",
    "Topic": "Audits",
    "Status": "Approved",
}


# ── generate_content_set unit tests ──────────────────────────────────────────

class TestGenerateContentSet:
    def test_returns_summary_dict(self):
        from marketing_content_generator import generate_content_set
        with patch("marketing_content_generator._call_claude", return_value=_SAMPLE_CONTENT), \
             patch("marketing_content_generator._write_content_queue_record", return_value="recNEW"):
            result = generate_content_set("recIDEA1", _IDEA_FIELDS)
        assert "created" in result
        assert "blocked" in result
        assert "platform_results" in result

    def test_creates_five_records_for_clean_content(self):
        from marketing_content_generator import generate_content_set
        with patch("marketing_content_generator._call_claude", return_value=_SAMPLE_CONTENT), \
             patch("marketing_content_generator._write_content_queue_record", return_value="recNEW"), \
             patch("marketing_content_generator._guard_text", return_value=(True, "PASS")):
            result = generate_content_set("recIDEA1", _IDEA_FIELDS)
        assert result["created"] == 5
        assert result["blocked"] == 0

    def test_blocked_content_increments_blocked_count(self):
        from marketing_content_generator import generate_content_set
        with patch("marketing_content_generator._call_claude", return_value=_SAMPLE_CONTENT), \
             patch("marketing_content_generator._guard_text", return_value=(False, "FAIL: banned-word:certified")):
            result = generate_content_set("recIDEA1", _IDEA_FIELDS)
        assert result["created"] == 0
        assert result["blocked"] == 5

    def test_returns_empty_when_claude_fails(self):
        from marketing_content_generator import generate_content_set
        with patch("marketing_content_generator._call_claude", return_value=None):
            result = generate_content_set("recIDEA1", _IDEA_FIELDS)
        assert result["created"] == 0
        assert result["blocked"] == 0

    def test_platform_results_has_correct_platforms(self):
        from marketing_content_generator import generate_content_set
        with patch("marketing_content_generator._call_claude", return_value=_SAMPLE_CONTENT), \
             patch("marketing_content_generator._write_content_queue_record", return_value="recNEW"), \
             patch("marketing_content_generator._guard_text", return_value=(True, "PASS")):
            result = generate_content_set("recIDEA1", _IDEA_FIELDS)
        platforms = {r["platform"] for r in result["platform_results"]}
        assert platforms == {"TikTok", "Facebook", "LinkedIn", "Email", "Website FAQ"}

    def test_tiktok_uses_bullets_not_body(self):
        """TikTok draft text should come from bullets (•-prefixed lines), not a body key."""
        from marketing_content_generator import generate_content_set
        captured = []

        def fake_write(idea_id, platform, draft_text, hooks, guard_result, needs_billy):
            captured.append((platform, draft_text))
            return "recX"

        with patch("marketing_content_generator._call_claude", return_value=_SAMPLE_CONTENT), \
             patch("marketing_content_generator._write_content_queue_record", side_effect=fake_write), \
             patch("marketing_content_generator._guard_text", return_value=(True, "PASS")):
            generate_content_set("recIDEA1", _IDEA_FIELDS)

        tiktok_drafts = [d for p, d in captured if p == "TikTok"]
        assert tiktok_drafts, "No TikTok record captured"
        assert "• Bullet one" in tiktok_drafts[0]

    def test_faq_uses_answer_not_body(self):
        from marketing_content_generator import generate_content_set
        captured = []

        def fake_write(idea_id, platform, draft_text, hooks, guard_result, needs_billy):
            captured.append((platform, draft_text))
            return "recX"

        with patch("marketing_content_generator._call_claude", return_value=_SAMPLE_CONTENT), \
             patch("marketing_content_generator._write_content_queue_record", side_effect=fake_write), \
             patch("marketing_content_generator._guard_text", return_value=(True, "PASS")):
            generate_content_set("recIDEA1", _IDEA_FIELDS)

        faq_drafts = [d for p, d in captured if p == "Website FAQ"]
        assert faq_drafts, "No FAQ record captured"
        assert "certificates on file" in faq_drafts[0]

    def test_needs_billy_passed_through(self):
        from marketing_content_generator import generate_content_set
        captured_flags = []

        def fake_write(idea_id, platform, draft_text, hooks, guard_result, needs_billy):
            captured_flags.append(needs_billy)
            return "recX"

        with patch("marketing_content_generator._call_claude", return_value=_SAMPLE_CONTENT), \
             patch("marketing_content_generator._write_content_queue_record", side_effect=fake_write), \
             patch("marketing_content_generator._guard_text", return_value=(True, "PASS")):
            generate_content_set("recIDEA1", _IDEA_FIELDS, needs_billy=True)

        assert all(f is True for f in captured_flags)

    def test_write_failure_does_not_crash(self):
        from marketing_content_generator import generate_content_set
        with patch("marketing_content_generator._call_claude", return_value=_SAMPLE_CONTENT), \
             patch("marketing_content_generator._write_content_queue_record", return_value=None), \
             patch("marketing_content_generator._guard_text", return_value=(True, "PASS")):
            result = generate_content_set("recIDEA1", _IDEA_FIELDS)
        assert result["created"] == 0


# ── _call_claude unit tests ───────────────────────────────────────────────────

class TestCallClaude:
    def test_parses_valid_json(self):
        import json
        from marketing_content_generator import _call_claude
        mock_resp = MagicMock()
        mock_resp.content = [MagicMock(text=json.dumps(_SAMPLE_CONTENT))]
        with patch("marketing_content_generator.anthropic.Anthropic") as mock_client_cls:
            mock_client_cls.return_value.messages.create.return_value = mock_resp
            result = _call_claude(_IDEA_FIELDS)
        assert result is not None
        assert "talking_points" in result

    def test_handles_json_in_code_fence(self):
        import json
        from marketing_content_generator import _call_claude
        fenced = "```json\n" + json.dumps(_SAMPLE_CONTENT) + "\n```"
        mock_resp = MagicMock()
        mock_resp.content = [MagicMock(text=fenced)]
        with patch("marketing_content_generator.anthropic.Anthropic") as mock_client_cls:
            mock_client_cls.return_value.messages.create.return_value = mock_resp
            result = _call_claude(_IDEA_FIELDS)
        assert result is not None

    def test_returns_none_on_api_error(self):
        from marketing_content_generator import _call_claude
        with patch("marketing_content_generator.anthropic.Anthropic") as mock_client_cls:
            mock_client_cls.return_value.messages.create.side_effect = Exception("API error")
            result = _call_claude(_IDEA_FIELDS)
        assert result is None

    def test_returns_none_on_invalid_json(self):
        from marketing_content_generator import _call_claude
        mock_resp = MagicMock()
        mock_resp.content = [MagicMock(text="not json at all")]
        with patch("marketing_content_generator.anthropic.Anthropic") as mock_client_cls:
            mock_client_cls.return_value.messages.create.return_value = mock_resp
            result = _call_claude(_IDEA_FIELDS)
        assert result is None


# ── _guard_text unit tests ────────────────────────────────────────────────────

class TestGuardText:
    def test_clean_text_passes(self):
        from marketing_content_generator import _guard_text
        ok, result = _guard_text("Certificates on file — the GC was ready when the audit came.")
        assert ok is True
        assert result == "PASS"

    def test_banned_word_fails(self):
        from marketing_content_generator import _guard_text
        ok, result = _guard_text("Your subcontractors are fully certified and compliant.")
        assert ok is False
        assert "FAIL" in result


# ── /generate route tests ─────────────────────────────────────────────────────

class TestGenerateRoute:
    def test_generate_requires_login(self, client):
        resp = client.post("/marketing/idea/recABC123/generate")
        assert resp.status_code == 302
        assert "login" in resp.headers.get("Location", "")

    def test_generate_redirects_to_marketing_on_success(self, authed_client):
        mock_idea = {
            "id": "recABC123",
            "fields": {**_IDEA_FIELDS},
        }
        with patch("review_app._api") as mock_api, \
             patch("marketing_content_generator.generate_content_set",
                   return_value={"created": 5, "blocked": 0, "platform_results": []}):
            mock_tbl = MagicMock()
            mock_tbl.get.return_value = mock_idea
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.post("/marketing/idea/recABC123/generate")
        assert resp.status_code == 302
        assert "/marketing" in resp.headers.get("Location", "")

    def test_generate_redirects_on_missing_idea(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.get.return_value = None
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.post("/marketing/idea/recMISSING/generate")
        assert resp.status_code == 302

    def test_generate_rejects_non_approved_idea(self, authed_client):
        mock_idea = {
            "id": "recABC123",
            "fields": {**_IDEA_FIELDS, "Status": "New"},
        }
        with patch("review_app._api") as mock_api:
            mock_tbl = MagicMock()
            mock_tbl.get.return_value = mock_idea
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.post("/marketing/idea/recABC123/generate")
        assert resp.status_code == 302

    def test_generate_handles_airtable_error(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_api.return_value.table.side_effect = Exception("Airtable down")
            resp = authed_client.post("/marketing/idea/recABC123/generate")
        assert resp.status_code == 302

    def test_generate_handles_generation_error(self, authed_client):
        mock_idea = {"id": "recABC123", "fields": {**_IDEA_FIELDS}}
        with patch("review_app._api") as mock_api, \
             patch("marketing_content_generator.generate_content_set",
                   side_effect=Exception("Claude API down")):
            mock_tbl = MagicMock()
            mock_tbl.get.return_value = mock_idea
            mock_api.return_value.table.return_value = mock_tbl
            resp = authed_client.post("/marketing/idea/recABC123/generate")
        assert resp.status_code == 302


# ── marketing page — content drafts display ───────────────────────────────────

class TestMarketingPageWithContent:
    def _make_idea_records(self):
        return [{
            "id": "recABC123",
            "fields": {
                "Idea": "Show how certificates on file saved audit day.",
                "Why It's a Story": "Audit stress is real.",
                "Audience": "GC",
                "Topic": "Audits",
                "Status": "Approved",
            }
        }]

    def _make_cq_records(self):
        return [{
            "id": "recCQ1",
            "fields": {
                "Platform": "LinkedIn",
                "Draft Text": "Here is a professional post about audits.",
                "Hooks": "Hook A\n---\nHook B\n---\nHook C",
                "Status": "Draft",
                "Compliance Guard Result": "PASS",
                "Tracked Link": "https://carolinacompliancesolutions.com/?utm_source=linkedin",
                "Needs Billy's Okay": False,
                "Idea Link": ["recABC123"],
            }
        }]

    def test_generate_button_shown_for_approved_idea_without_content(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_ideas_tbl = MagicMock()
            mock_ideas_tbl.all.return_value = self._make_idea_records()
            mock_cq_tbl = MagicMock()
            mock_cq_tbl.all.return_value = []  # no content yet

            def table_factory(base_id, table_id):
                from marketing_constants import CONTENT_QUEUE_TABLE_ID
                if table_id == CONTENT_QUEUE_TABLE_ID:
                    return mock_cq_tbl
                return mock_ideas_tbl

            mock_api.return_value.table.side_effect = table_factory
            resp = authed_client.get("/marketing")
        assert b"Generate content set" in resp.data

    def test_generate_button_hidden_when_drafts_exist(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_ideas_tbl = MagicMock()
            mock_ideas_tbl.all.return_value = self._make_idea_records()
            mock_cq_tbl = MagicMock()
            mock_cq_tbl.all.return_value = self._make_cq_records()

            def table_factory(base_id, table_id):
                from marketing_constants import CONTENT_QUEUE_TABLE_ID
                if table_id == CONTENT_QUEUE_TABLE_ID:
                    return mock_cq_tbl
                return mock_ideas_tbl

            mock_api.return_value.table.side_effect = table_factory
            resp = authed_client.get("/marketing")
        assert b"Generate content set" not in resp.data
        assert b"Content Drafts" in resp.data

    def test_draft_platform_shown(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_ideas_tbl = MagicMock()
            mock_ideas_tbl.all.return_value = self._make_idea_records()
            mock_cq_tbl = MagicMock()
            mock_cq_tbl.all.return_value = self._make_cq_records()

            def table_factory(base_id, table_id):
                from marketing_constants import CONTENT_QUEUE_TABLE_ID
                if table_id == CONTENT_QUEUE_TABLE_ID:
                    return mock_cq_tbl
                return mock_ideas_tbl

            mock_api.return_value.table.side_effect = table_factory
            resp = authed_client.get("/marketing")
        assert b"LinkedIn" in resp.data

    def test_guard_pass_badge_shown(self, authed_client):
        with patch("review_app._api") as mock_api:
            mock_ideas_tbl = MagicMock()
            mock_ideas_tbl.all.return_value = self._make_idea_records()
            mock_cq_tbl = MagicMock()
            mock_cq_tbl.all.return_value = self._make_cq_records()

            def table_factory(base_id, table_id):
                from marketing_constants import CONTENT_QUEUE_TABLE_ID
                if table_id == CONTENT_QUEUE_TABLE_ID:
                    return mock_cq_tbl
                return mock_ideas_tbl

            mock_api.return_value.table.side_effect = table_factory
            resp = authed_client.get("/marketing")
        assert b"Guard: Pass" in resp.data
