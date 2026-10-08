from types import SimpleNamespace as NS

import extractor


def test_default_model_is_sonnet():
    assert extractor.EXTRACTION_MODEL == "claude-sonnet-5-5"


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
