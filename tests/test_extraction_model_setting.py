from types import SimpleNamespace as NS
from pathlib import Path
from unittest.mock import MagicMock, patch
import json

import extractor


def test_default_model_is_sonnet():
    assert extractor.EXTRACTION_MODEL == "claude-sonnet-5-5"


def test_extraction_call_uses_temperature_zero(tmp_path):
    """Main extraction API call must pass temperature=0 for deterministic results."""
    fake_pdf = tmp_path / "cert.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4")

    mock_response = MagicMock()
    mock_response.content = [NS(type="text", text='{"document_type": "COI", "policies": []}')]

    mock_messages = MagicMock()
    mock_messages.create.return_value = mock_response

    mock_client = MagicMock()
    mock_client.messages = mock_messages

    with patch("extractor.anthropic.Anthropic", return_value=mock_client), \
         patch("extractor.build_message_content", return_value=[]), \
         patch("extractor.pdf_bundle.pdf_has_text_layer", return_value=True), \
         patch("extractor.ANTHROPIC_API_KEY", "test-key"):
        extractor.extract_document(fake_pdf)

    call_kwargs = mock_messages.create.call_args[1]
    assert call_kwargs.get("temperature") == 0


def test_response_text_skips_thinking_blocks():
    resp = NS(content=[NS(type="thinking", thinking="..."), NS(type="text", text=' {"a": 1} ')])
    assert extractor._response_text(resp) == '{"a": 1}'


def test_response_text_plain_reply():
    assert extractor._response_text(NS(content=[NS(type="text", text="hi")])) == "hi"


def test_system_prompt_contains_checkbox_rules():
    assert extractor.CHECKBOX_RULES in extractor.SYSTEM_PROMPT


def test_system_prompt_contains_umbrella_rule():
    assert "UMBRELLA LIAB / EXCESS LIAB" in extractor.SYSTEM_PROMPT


def test_checkbox_rules_before_retroactive_date():
    idx_checkbox = extractor.SYSTEM_PROMPT.index("CHECKBOX POSITION")
    idx_retro = extractor.SYSTEM_PROMPT.index("- RETROACTIVE DATE:")
    assert idx_checkbox < idx_retro
