"""Tests for contacts pre-fill logic.

_get_vendor_contact is a pure function (no I/O), so we test the behaviour inline
rather than importing through the full Flask app (which requires REVIEW_PASSWORD).
"""
import pytest


def _get_vendor_contact(vfields: dict) -> dict:
    """Pure helper — mirrors review_app._get_vendor_contact exactly."""
    agent_email  = (vfields.get("Agent Email") or "").strip()
    agent_name   = (vfields.get("Agent Name") or "").strip()
    agent_phone  = (vfields.get("Agent Phone") or "").strip()
    agent_agency = (vfields.get("Agent Agency") or "").strip()
    vendor_email = (vfields.get("Vendor Email") or "").strip()
    primary      = (vfields.get("Primary Contact") or "Agent").strip()
    bounced      = bool(vfields.get("Agent Bounced"))

    use_agent = agent_email and not bounced and (primary == "Agent" or not vendor_email)
    if use_agent:
        return {
            "type": "agent", "email": agent_email,
            "name": agent_name or "Insurance Agent",
            "phone": agent_phone, "agency": agent_agency,
        }
    if vendor_email:
        return {
            "type": "vendor", "email": vendor_email,
            "name": (vfields.get("Vendor Name") or "").strip(),
            "phone": "", "agency": "",
        }
    return {"type": "none", "email": "", "name": "", "phone": "", "agency": ""}


# ── basic contact resolution ──────────────────────────────────────────────────

def test_agent_email_returns_agent_type():
    contact = _get_vendor_contact({
        "Agent Email": "agent@ins.com",
        "Agent Name": "Jane Smith",
        "Agent Phone": "555-1234",
        "Agent Agency": "Smith Insurance",
        "Vendor Email": "",
        "Primary Contact": "Agent",
        "Agent Bounced": False,
    })
    assert contact["type"] == "agent"
    assert contact["email"] == "agent@ins.com"
    assert contact["name"] == "Jane Smith"
    assert contact["agency"] == "Smith Insurance"


def test_vendor_email_used_when_no_agent():
    contact = _get_vendor_contact({
        "Agent Email": "",
        "Vendor Email": "owner@sub.com",
        "Vendor Name": "Sub LLC",
        "Primary Contact": "Agent",
        "Agent Bounced": False,
    })
    assert contact["type"] == "vendor"
    assert contact["email"] == "owner@sub.com"


def test_no_emails_returns_none_type():
    contact = _get_vendor_contact({
        "Agent Email": "",
        "Vendor Email": "",
        "Agent Bounced": False,
    })
    assert contact["type"] == "none"
    assert contact["email"] == ""


def test_bounced_agent_falls_back_to_vendor_email():
    contact = _get_vendor_contact({
        "Agent Email": "agent@ins.com",
        "Agent Bounced": True,
        "Vendor Email": "owner@sub.com",
        "Vendor Name": "Sub LLC",
        "Primary Contact": "Agent",
    })
    assert contact["type"] == "vendor"
    assert contact["email"] == "owner@sub.com"


def test_bounced_agent_no_vendor_returns_none():
    contact = _get_vendor_contact({
        "Agent Email": "agent@ins.com",
        "Agent Bounced": True,
        "Vendor Email": "",
        "Primary Contact": "Agent",
    })
    assert contact["type"] == "none"


def test_vendor_primary_contact_prefers_vendor_email():
    contact = _get_vendor_contact({
        "Agent Email": "agent@ins.com",
        "Agent Bounced": False,
        "Vendor Email": "owner@sub.com",
        "Vendor Name": "Sub LLC",
        "Primary Contact": "Vendor",
    })
    assert contact["type"] == "vendor"
    assert contact["email"] == "owner@sub.com"


def test_agent_name_defaults_when_blank():
    contact = _get_vendor_contact({
        "Agent Email": "agent@ins.com",
        "Agent Name": "",
        "Agent Bounced": False,
        "Primary Contact": "Agent",
    })
    assert contact["type"] == "agent"
    assert contact["name"] == "Insurance Agent"


# ── contacts pre-fill summary logic ──────────────────────────────────────────

def _contacts_summary(subs_needing_cert: list, vendor_fields_map: dict) -> dict:
    """Mirrors the contacts-summary logic in _compute_audit_analysis."""
    missing = []
    with_contact = 0
    for sub in subs_needing_cert:
        vid     = sub.get("vendor_id", "")
        vfields = vendor_fields_map.get(vid, {})
        contact = _get_vendor_contact(vfields)
        if contact["type"] == "none":
            missing.append({
                "payee_raw":   sub["payee_raw"],
                "vendor_name": sub["vendor_name"],
                "vendor_id":   vid,
            })
        else:
            with_contact += 1
    return {
        "subs_needing_cert": len(subs_needing_cert),
        "with_contact":      with_contact,
        "missing_contact":   missing,
    }


def test_contacts_summary_all_have_contact():
    subs = [
        {"payee_raw": "Alpha LLC", "vendor_name": "Alpha LLC", "vendor_id": "v1"},
        {"payee_raw": "Beta Inc",  "vendor_name": "Beta Inc",  "vendor_id": "v2"},
    ]
    vmap = {
        "v1": {"Agent Email": "agent1@ins.com", "Agent Bounced": False, "Primary Contact": "Agent"},
        "v2": {"Vendor Email": "beta@sub.com"},
    }
    result = _contacts_summary(subs, vmap)
    assert result["with_contact"] == 2
    assert result["missing_contact"] == []


def test_contacts_summary_one_missing():
    subs = [
        {"payee_raw": "Alpha LLC", "vendor_name": "Alpha LLC", "vendor_id": "v1"},
        {"payee_raw": "Ghost Inc", "vendor_name": "Ghost Inc",  "vendor_id": "v2"},
    ]
    vmap = {
        "v1": {"Agent Email": "agent1@ins.com", "Agent Bounced": False, "Primary Contact": "Agent"},
        "v2": {},  # no contact info
    }
    result = _contacts_summary(subs, vmap)
    assert result["with_contact"] == 1
    assert len(result["missing_contact"]) == 1
    assert result["missing_contact"][0]["vendor_name"] == "Ghost Inc"


def test_contacts_summary_empty_subs():
    result = _contacts_summary([], {})
    assert result["subs_needing_cert"] == 0
    assert result["with_contact"] == 0
    assert result["missing_contact"] == []
