"""Tests for utm_builder.py."""
import urllib.parse
from utm_builder import build_utm_link, UTM_CAMPAIGN


class TestBuildUtmLink:

    def test_returns_valid_url(self):
        url = build_utm_link("Five things GCs miss on their subs' certs", "LinkedIn")
        parsed = urllib.parse.urlparse(url)
        assert parsed.scheme == "https"
        assert "carolinacompliancesolutions.com" in parsed.netloc or "carolinacompliancesolutions.com" in parsed.path.lower() or url.startswith("https://carolinacompliancesolutions.com")

    def test_utm_params_present(self):
        url = build_utm_link("Test idea", "LinkedIn")
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
        assert params.get("utm_source") == "linkedin"
        assert params.get("utm_medium") == "social"
        assert params.get("utm_campaign") == UTM_CAMPAIGN
        assert "utm_content" in params

    def test_facebook_medium_social(self):
        url = build_utm_link("Test idea", "Facebook")
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
        assert params["utm_medium"] == "social"
        assert params["utm_source"] == "facebook"

    def test_email_medium(self):
        url = build_utm_link("Test idea", "Email")
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
        assert params["utm_medium"] == "email"

    def test_demo_page(self):
        url = build_utm_link("Test", "LinkedIn", page="demo")
        assert "demo" in url

    def test_audit_page(self):
        url = build_utm_link("Test", "LinkedIn", page="audit")
        assert "audit" in url

    def test_different_ideas_produce_different_links(self):
        url1 = build_utm_link("Five things GCs miss", "LinkedIn")
        url2 = build_utm_link("Why audits are easier with certs on file", "LinkedIn")
        assert url1 != url2

    def test_same_idea_same_platform_idempotent(self):
        url1 = build_utm_link("Consistent idea text", "Facebook")
        url2 = build_utm_link("Consistent idea text", "Facebook")
        assert url1 == url2

    def test_utm_content_uses_idea_slug(self):
        # UTM content is derived from the idea text; actual guard runs before build_utm_link is called
        url = build_utm_link("Five things GCs miss on certs", "TikTok")
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
        content = params.get("utm_content", "")
        assert len(content) > 0
        assert "five" in content or "things" in content or "gcs" in content or "miss" in content
