"""Tests for marketing_claim_checker.py — mocks the Anthropic call."""
import sys
import os
import json
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import marketing_claim_checker as cc


SAMPLE_PAGES = [
    {"title": "Home", "body_text": "Carolina Compliance Solutions helps GCs track COIs. Simple online dashboard."},
    {"title": "Pricing", "body_text": "Plans start at $99/month. Unlimited vendors included."},
]


def _mock_response(flags: list[str]):
    msg = MagicMock()
    msg.content = [MagicMock(text=json.dumps(flags))]
    return msg


# ── _build_kb_context ─────────────────────────────────────────────────────────

def test_build_kb_context_includes_titles():
    ctx = cc._build_kb_context(SAMPLE_PAGES)
    assert "Home" in ctx
    assert "Pricing" in ctx


def test_build_kb_context_truncates():
    big_pages = [{"title": "Big", "body_text": "x" * (cc.MAX_KB_CHARS + 1000)}]
    ctx = cc._build_kb_context(big_pages)
    assert len(ctx) <= cc.MAX_KB_CHARS + 100  # small overhead for title


def test_build_kb_context_empty():
    ctx = cc._build_kb_context([])
    assert ctx == ""


# ── check_claims ──────────────────────────────────────────────────────────────

def test_check_claims_returns_flags(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        client.messages.create.return_value = _mock_response(["We offer 24/7 support"])
        flags = cc.check_claims("We offer 24/7 support.", SAMPLE_PAGES)
    assert flags == ["We offer 24/7 support"]


def test_check_claims_empty_on_pass(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        client.messages.create.return_value = _mock_response([])
        flags = cc.check_claims("Simple dashboard for COIs.", SAMPLE_PAGES)
    assert flags == []


def test_check_claims_no_api_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    flags = cc.check_claims("Some draft text.", SAMPLE_PAGES)
    assert flags == []


def test_check_claims_empty_draft(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    flags = cc.check_claims("", SAMPLE_PAGES)
    assert flags == []


def test_check_claims_no_kb_pages(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    flags = cc.check_claims("Some draft.", [])
    assert flags == []


def test_check_claims_handles_api_error(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        client.messages.create.side_effect = Exception("API error")
        flags = cc.check_claims("Some draft.", SAMPLE_PAGES)
    assert flags == []


def test_check_claims_handles_bad_json(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        msg = MagicMock()
        msg.content = [MagicMock(text="not json at all")]
        client.messages.create.return_value = msg
        flags = cc.check_claims("Some draft.", SAMPLE_PAGES)
    assert flags == []


def test_check_claims_strips_code_fence(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    with patch("anthropic.Anthropic") as MockClient:
        client = MockClient.return_value
        msg = MagicMock()
        msg.content = [MagicMock(text='```json\n["flagged claim"]\n```')]
        client.messages.create.return_value = msg
        flags = cc.check_claims("Some draft.", SAMPLE_PAGES)
    assert flags == ["flagged claim"]
