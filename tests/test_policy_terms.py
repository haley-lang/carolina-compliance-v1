"""Each policy term is its own record: renewals and backlog certificates never
overwrite or discard an earlier term (audit history).

Example data: Lenos Construction renewed the same policy numbers for two
consecutive terms (11/09/2024-11/09/2025 and 11/09/2025-11/09/2026).
"""
import re

import processor
from processor import get_existing_policy_for_term, process_policies

VENDOR = "recVENDOR1"
CERT = "recCERT1"


class FakePolicies:
    """Minimal in-memory stand-in for the Insurance Policies table."""

    def __init__(self, records=None):
        self.records = {r["id"]: r for r in (records or [])}
        self.created = []
        self.updated = []
        self._next = 1

    def first(self, formula=None, **kw):
        found = self.all(formula=formula)
        return found[0] if found else None

    def all(self, formula=None, **kw):
        m = re.match(r"\{Policy Number\} = '(.*)'$", formula or "")
        if m:
            num = m.group(1)
            return [r for r in self.records.values() if r["fields"].get("Policy Number") == num]
        if (formula or "").startswith("AND(FIND("):
            types = set(re.findall(r"\{Policy Type\} = '([^']*)'", formula))
            return [
                r for r in self.records.values()
                if VENDOR in (r["fields"].get("Vendor Link") or [])
                and r["fields"].get("Policy Type") in types
                and r["fields"].get("Status") != "Superseded"
            ]
        return list(self.records.values())

    def update(self, rid, fields, typecast=False):
        self.records[rid]["fields"].update(fields)
        self.updated.append((rid, dict(fields)))
        return self.records[rid]

    def create(self, fields, typecast=False):
        rid = f"recNEW{self._next}"
        self._next += 1
        self.records[rid] = {"id": rid, "fields": dict(fields)}
        self.created.append(rid)
        return self.records[rid]


def _policy(number, eff, exp, ptype="COMMERCIAL GENERAL LIABILITY"):
    return {
        "policy_number": number, "policy_type": ptype, "carrier": "Erie Insurance Company",
        "effective_date": eff, "expiration_date": exp, "coverage_limits": "EACH OCCURRENCE $1,000,000",
    }


def _existing(rid, number, eff, exp, status="Current", ptype="General Liability"):
    return {"id": rid, "fields": {
        "Policy Number": number, "Policy Type": ptype, "Effective Date": eff,
        "Expiration Date": exp, "Status": status, "Vendor Link": [VENDOR],
        "Insurance Certificates": [CERT],
    }}


def _run(table, policies):
    return process_policies(table, policies, VENDOR, CERT, "cert.pdf")


# ── lookup by term ───────────────────────────────────────────────────────────

def test_lookup_same_number_different_effective_date_is_a_different_term():
    t = FakePolicies([_existing("recOLD", "Q61-0338745", "2024-11-09", "2025-11-09")])
    assert get_existing_policy_for_term(t, "Q61-0338745", "2025-11-09") is None


def test_lookup_same_number_same_effective_date_is_same_term():
    t = FakePolicies([_existing("recOLD", "Q61-0338745", "2024-11-09", "2025-11-09")])
    assert get_existing_policy_for_term(t, "Q61-0338745", "11/09/2024")["id"] == "recOLD"


def test_lookup_without_incoming_effective_date_falls_back_to_number():
    t = FakePolicies([_existing("recOLD", "Q61-0338745", "2024-11-09", "2025-11-09")])
    assert get_existing_policy_for_term(t, "Q61-0338745", "")["id"] == "recOLD"


def test_lookup_record_on_file_without_effective_date_counts_as_same_term():
    t = FakePolicies([_existing("recOLD", "Q61-0338745", "", "2025-11-09")])
    assert get_existing_policy_for_term(t, "Q61-0338745", "2025-11-09")["id"] == "recOLD"


# ── processing ───────────────────────────────────────────────────────────────

def test_renewal_with_same_policy_number_creates_new_term_and_keeps_old_dates():
    t = FakePolicies([_existing("recOLD", "Q61-0338745", "2024-11-09", "2025-11-09")])
    _run(t, [_policy("Q61-0338745", "2025-11-09", "2026-11-09")])

    assert len(t.created) == 1, "renewal must create a new record"
    old = t.records["recOLD"]["fields"]
    assert old["Expiration Date"] == "2025-11-09", "older term's dates must not be overwritten"
    assert old["Status"] == "Superseded"
    new = t.records[t.created[0]]["fields"]
    assert new["Effective Date"] == "2025-11-09" and new["Expiration Date"] == "2026-11-09"
    assert new["Status"] != "Superseded"


def test_older_backlog_certificate_is_stored_as_superseded_history_not_discarded():
    t = FakePolicies([_existing("recCUR", "Q61-0338745", "2025-11-09", "2026-11-09")])
    _run(t, [_policy("Q61-0338745", "2024-11-09", "2025-11-09")])

    assert len(t.created) == 1, "older term must be stored"
    hist = t.records[t.created[0]]["fields"]
    assert hist["Status"] == "Superseded"
    assert hist["Effective Date"] == "2024-11-09" and hist["Expiration Date"] == "2025-11-09"
    cur = t.records["recCUR"]["fields"]
    assert cur["Expiration Date"] == "2026-11-09", "current term must not be touched"
    assert cur["Status"] == "Current"


def test_older_term_with_different_policy_number_also_stored_as_history():
    t = FakePolicies([_existing("recCUR", "NEW-222", "2025-11-09", "2026-11-09")])
    _run(t, [_policy("OLD-111", "2024-11-09", "2025-11-09")])
    assert t.records[t.created[0]]["fields"]["Status"] == "Superseded"
    assert t.records["recCUR"]["fields"]["Status"] == "Current"


def test_resubmitted_same_term_refreshes_and_does_not_create_a_second_record():
    t = FakePolicies([_existing("recCUR", "Q61-0338745", "2025-11-09", "2026-11-09")])
    _run(t, [_policy("Q61-0338745", "2025-11-09", "2026-11-09")])
    assert t.created == []
    assert any(rid == "recCUR" for rid, _ in t.updated)


def test_two_consecutive_terms_processed_in_either_order_give_same_result():
    old = _policy("Q61-0338745", "2024-11-09", "2025-11-09")
    new = _policy("Q61-0338745", "2025-11-09", "2026-11-09")
    for order in ([old, new], [new, old]):
        t = FakePolicies()
        for p in order:
            _run(t, [p])
        recs = list(t.records.values())
        assert len(recs) == 2
        by_eff = {r["fields"]["Effective Date"]: r["fields"] for r in recs}
        assert by_eff["2024-11-09"]["Status"] == "Superseded"
        assert by_eff["2024-11-09"]["Expiration Date"] == "2025-11-09"
        assert by_eff["2025-11-09"]["Status"] != "Superseded"
        assert by_eff["2025-11-09"]["Expiration Date"] == "2026-11-09"
