"""
Site knowledge base — fetch, clean, and store public CCS website pages.
Reads the sitemap, scrapes each page, and upserts records to the
Site Knowledge Airtable table.  No external dependencies beyond stdlib.
"""
from __future__ import annotations
import logging
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from xml.etree import ElementTree

import pyairtable

from marketing_constants import (
    MARKETING_BASE_ID,
    SITE_KNOWLEDGE_TABLE_ID,
    SKB_URL, SKB_TITLE, SKB_BODY_TEXT, SKB_LAST_FETCHED, SKB_FETCH_STATUS,
    SKB_STATUS_OK, SKB_STATUS_ERROR, SKB_STATUS_PENDING,
)

log = logging.getLogger(__name__)

SITE_BASE = "https://carolinacompliancesolutions.com"
SITEMAP_PATHS = ["/sitemap.xml", "/sitemap_index.xml", "/page-sitemap.xml"]
FETCH_TIMEOUT = 15
MAX_BODY_CHARS = 8000  # truncate stored body to control Airtable field size


# ── HTML cleaning ─────────────────────────────────────────────────────────────

_SKIP_TAGS = {"script", "style", "nav", "footer", "head", "noscript", "svg", "iframe"}


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._chunks: list[str] = []
        self._skip_depth: int = 0
        self._title_buf: list[str] = []
        self._in_title: bool = False

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in _SKIP_TAGS:
            self._skip_depth += 1
        if tag == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag in _SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._title_buf.append(data)
        elif self._skip_depth == 0:
            self._chunks.append(data)

    @property
    def title(self) -> str:
        raw = "".join(self._title_buf).strip()
        # Strip " | CCS" or " - Carolina Compliance Solutions" suffixes
        raw = re.sub(r"\s*[\|–\-]\s*Carolina Compliance Solutions.*$", "", raw, flags=re.I)
        raw = re.sub(r"\s*[\|–\-]\s*CCS$", "", raw, flags=re.I)
        return raw.strip()

    @property
    def body(self) -> str:
        text = " ".join(self._chunks)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:MAX_BODY_CHARS]


def _clean_html(html_bytes: bytes) -> tuple[str, str]:
    """Return (title, body_text) from raw HTML bytes."""
    try:
        html = html_bytes.decode("utf-8", errors="replace")
    except Exception:
        return "", ""
    p = _TextExtractor()
    try:
        p.feed(html)
    except Exception:
        pass
    return p.title, p.body


# ── Sitemap parsing ───────────────────────────────────────────────────────────

def _fetch_bytes(url: str) -> bytes | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CCS-KB-Bot/1.0"})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            return resp.read()
    except (urllib.error.URLError, OSError) as exc:
        log.debug("fetch failed %s: %s", url, exc)
        return None


def _parse_sitemap(xml_bytes: bytes) -> list[str]:
    """Extract <loc> URLs from a sitemap or sitemap index XML."""
    urls: list[str] = []
    try:
        root = ElementTree.fromstring(xml_bytes)
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"
        for elem in root.iter(f"{ns}loc"):
            val = (elem.text or "").strip()
            if val:
                urls.append(val)
    except Exception as exc:
        log.debug("sitemap parse failed: %s", exc)
    return urls


def fetch_sitemap_urls() -> list[str]:
    """Try known sitemap paths, return a deduplicated list of page URLs."""
    all_urls: list[str] = []
    for path in SITEMAP_PATHS:
        url = SITE_BASE + path
        raw = _fetch_bytes(url)
        if raw is None:
            continue
        candidates = _parse_sitemap(raw)
        for u in candidates:
            if u.endswith(".xml"):
                # It's a sitemap index pointing to child sitemaps
                child = _fetch_bytes(u)
                if child:
                    all_urls.extend(_parse_sitemap(child))
            else:
                all_urls.append(u)
        if all_urls:
            break

    # Deduplicate, keep only site pages (not images/pdfs)
    seen: set[str] = set()
    result: list[str] = []
    for u in all_urls:
        u = u.rstrip("/")
        skip_exts = (".jpg", ".jpeg", ".png", ".gif", ".pdf", ".svg", ".xml")
        if any(u.lower().endswith(e) for e in skip_exts):
            continue
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


# ── Airtable I/O ──────────────────────────────────────────────────────────────

def _get_existing_records(tbl) -> dict[str, str]:
    """Return {url: record_id} for all existing Site Knowledge records."""
    result: dict[str, str] = {}
    try:
        for rec in tbl.all(fields=[SKB_URL]):
            url = (rec.get("fields", {}).get(SKB_URL) or "").rstrip("/")
            if url:
                result[url] = rec["id"]
    except Exception as exc:
        log.warning("site_knowledge: failed to load existing records: %s", exc)
    return result


def upsert_page(tbl, url: str, title: str, body_text: str, status: str = SKB_STATUS_OK) -> str | None:
    """Create or update a Site Knowledge record. Returns record ID or None."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    fields = {
        SKB_URL:          url,
        SKB_TITLE:        title or url,
        SKB_BODY_TEXT:    body_text,
        SKB_LAST_FETCHED: now_str,
        SKB_FETCH_STATUS: status,
    }
    existing = _get_existing_records(tbl)
    norm = url.rstrip("/")
    try:
        if norm in existing:
            tbl.update(existing[norm], fields)
            return existing[norm]
        else:
            rec = tbl.create(fields)
            return rec["id"]
    except Exception as exc:
        log.warning("site_knowledge: upsert failed for %s: %s", url, exc)
        return None


def refresh_all(api=None) -> dict:
    """
    Fetch all public CCS pages and upsert to Site Knowledge table.
    Returns summary: {pages_found, updated, errors}.
    """
    if api is None:
        api = pyairtable.Api(os.environ["AIRTABLE_API_KEY"])
    tbl = api.table(MARKETING_BASE_ID, SITE_KNOWLEDGE_TABLE_ID)

    urls = fetch_sitemap_urls()
    if not urls:
        log.warning("site_knowledge: no URLs found in sitemap; trying homepage")
        urls = [SITE_BASE]

    summary = {"pages_found": len(urls), "updated": 0, "errors": 0}
    existing = _get_existing_records(tbl)

    for url in urls:
        try:
            raw = _fetch_bytes(url)
            if raw is None:
                raise ValueError("empty response")
            title, body_text = _clean_html(raw)
            norm = url.rstrip("/")
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            fields = {
                SKB_URL:          url,
                SKB_TITLE:        title or url,
                SKB_BODY_TEXT:    body_text,
                SKB_LAST_FETCHED: now_str,
                SKB_FETCH_STATUS: SKB_STATUS_OK,
            }
            if norm in existing:
                tbl.update(existing[norm], fields)
            else:
                tbl.create(fields)
            summary["updated"] += 1
            time.sleep(0.25)  # polite crawl rate
        except Exception as exc:
            log.warning("site_knowledge: error fetching %s: %s", url, exc)
            summary["errors"] += 1

    return summary


def get_all_pages(api=None) -> list[dict]:
    """
    Load all successfully-fetched KB pages from Airtable.
    Returns list of {url, title, body_text}.
    """
    if api is None:
        api = pyairtable.Api(os.environ["AIRTABLE_API_KEY"])
    tbl = api.table(MARKETING_BASE_ID, SITE_KNOWLEDGE_TABLE_ID)
    pages: list[dict] = []
    try:
        records = tbl.all(
            formula=f"{{{SKB_FETCH_STATUS}}}='{SKB_STATUS_OK}'",
            fields=[SKB_URL, SKB_TITLE, SKB_BODY_TEXT],
        )
        for rec in records:
            f = rec.get("fields", {})
            pages.append({
                "url":       f.get(SKB_URL, ""),
                "title":     f.get(SKB_TITLE, ""),
                "body_text": f.get(SKB_BODY_TEXT, ""),
            })
    except Exception as exc:
        log.warning("site_knowledge: get_all_pages failed: %s", exc)
    return pages


def get_page_list(api=None) -> list[dict]:
    """Return lightweight page list (no body) for display in the UI."""
    if api is None:
        api = pyairtable.Api(os.environ["AIRTABLE_API_KEY"])
    tbl = api.table(MARKETING_BASE_ID, SITE_KNOWLEDGE_TABLE_ID)
    pages: list[dict] = []
    try:
        records = tbl.all(fields=[SKB_URL, SKB_TITLE, SKB_LAST_FETCHED, SKB_FETCH_STATUS])
        for rec in records:
            f = rec.get("fields", {})
            pages.append({
                "url":          f.get(SKB_URL, ""),
                "title":        f.get(SKB_TITLE, ""),
                "last_fetched": f.get(SKB_LAST_FETCHED, ""),
                "status":       f.get(SKB_FETCH_STATUS, ""),
            })
    except Exception as exc:
        log.warning("site_knowledge: get_page_list failed: %s", exc)
    return pages
