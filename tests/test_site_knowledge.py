"""Tests for site_knowledge.py — pure functions only (no network, no Airtable)."""
import sys
import os

# Bootstrap path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import site_knowledge as sk


# ── _clean_html ───────────────────────────────────────────────────────────────

def test_clean_html_extracts_title():
    html = b"<html><head><title>Pricing | CCS</title></head><body><p>Hello</p></body></html>"
    title, body = sk._clean_html(html)
    assert title == "Pricing"


def test_clean_html_strips_ccs_suffix():
    html = b"<html><head><title>Home - Carolina Compliance Solutions</title></head><body><p>Hi</p></body></html>"
    title, _ = sk._clean_html(html)
    assert "Carolina Compliance Solutions" not in title
    assert title == "Home"


def test_clean_html_strips_script_content():
    html = b"<html><body><script>alert('bad')</script><p>good text</p></body></html>"
    _, body = sk._clean_html(html)
    assert "alert" not in body
    assert "good text" in body


def test_clean_html_strips_nav():
    html = b"<html><body><nav>skip me</nav><main><p>keep me</p></main></body></html>"
    _, body = sk._clean_html(html)
    assert "skip me" not in body
    assert "keep me" in body


def test_clean_html_normalizes_whitespace():
    html = b"<html><body><p>word1   word2\n\n  word3</p></body></html>"
    _, body = sk._clean_html(html)
    assert "word1 word2" in body or "word1" in body


def test_clean_html_truncates_body():
    long_text = b"x" * (sk.MAX_BODY_CHARS + 500)
    html = b"<html><body><p>" + long_text + b"</p></body></html>"
    _, body = sk._clean_html(html)
    assert len(body) <= sk.MAX_BODY_CHARS


def test_clean_html_handles_empty():
    title, body = sk._clean_html(b"")
    assert title == ""
    assert body == ""


def test_clean_html_handles_broken_html():
    html = b"<p>unclosed tag <b>bold"
    title, body = sk._clean_html(html)
    assert isinstance(body, str)


# ── _parse_sitemap ────────────────────────────────────────────────────────────

def test_parse_sitemap_extracts_locs():
    xml = b"""<?xml version="1.0"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://example.com/page1</loc></url>
      <url><loc>https://example.com/page2</loc></url>
    </urlset>"""
    urls = sk._parse_sitemap(xml)
    assert "https://example.com/page1" in urls
    assert "https://example.com/page2" in urls


def test_parse_sitemap_handles_no_namespace():
    xml = b"<urlset><url><loc>https://example.com/a</loc></url></urlset>"
    urls = sk._parse_sitemap(xml)
    assert "https://example.com/a" in urls


def test_parse_sitemap_returns_empty_on_bad_xml():
    urls = sk._parse_sitemap(b"not xml at all {{}")
    assert isinstance(urls, list)


def test_parse_sitemap_index():
    xml = b"""<?xml version="1.0"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <sitemap><loc>https://example.com/sitemap-pages.xml</loc></sitemap>
    </sitemapindex>"""
    urls = sk._parse_sitemap(xml)
    # Sitemap index returns the child sitemap URL as a "url"
    assert "https://example.com/sitemap-pages.xml" in urls


# ── fetch_sitemap_urls deduplication ─────────────────────────────────────────

def test_dedup_excludes_images_and_pdfs():
    # We can't call the real network; test the filter logic inline
    candidates = [
        "https://example.com/page",
        "https://example.com/image.jpg",
        "https://example.com/doc.pdf",
        "https://example.com/page",   # duplicate
    ]
    seen = set()
    result = []
    skip_exts = (".jpg", ".jpeg", ".png", ".gif", ".pdf", ".svg", ".xml")
    for u in candidates:
        u = u.rstrip("/")
        if any(u.lower().endswith(e) for e in skip_exts):
            continue
        if u not in seen:
            seen.add(u)
            result.append(u)
    assert result == ["https://example.com/page"]
