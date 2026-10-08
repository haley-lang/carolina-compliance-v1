from types import SimpleNamespace as NS

import extractor


def test_default_model_unchanged():
    assert extractor.EXTRACTION_MODEL == "claude-opus-4-5"


def test_response_text_skips_thinking_blocks():
    resp = NS(content=[NS(type="thinking", thinking="..."), NS(type="text", text=' {"a": 1} ')])
    assert extractor._response_text(resp) == '{"a": 1}'


def test_response_text_plain_reply():
    assert extractor._response_text(NS(content=[NS(type="text", text="hi")])) == "hi"
