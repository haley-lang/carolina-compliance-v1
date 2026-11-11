"""Tests for compliance log dedup logic in module_7b_requirement_validator.

Verifies that write_compliance_log only writes a new row when the decision or
failure reasons have changed from the previous entry for that vendor+client pair.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import sys
import os

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_log_table():
    """Return a mock compliance log table that tracks .create() calls."""
    tbl = MagicMock()
    tbl._created = []

    def _create(fields, **kwargs):
        tbl._created.append(fields)
        return {"id": f"recMock{len(tbl._created)}", "fields": fields}

    tbl.create.side_effect = _create
    return tbl


def _import_fn():
    """Import the two functions under test, patching health_store away."""
    with patch.dict("sys.modules", {"health_store": MagicMock()}):
        import importlib
        import module_7b_requirement_validator as m7b
        importlib.reload(m7b)
        return m7b._build_last_log_cache, m7b.write_compliance_log


# ---------------------------------------------------------------------------
# _build_last_log_cache
# ---------------------------------------------------------------------------

class TestBuildLastLogCache:
    def test_empty_table_returns_empty_dict(self):
        _, _ = _import_fn()
        import module_7b_requirement_validator as m7b

        tbl = MagicMock()
        tbl.all.return_value = []

        result = m7b._build_last_log_cache(tbl)
        assert result == {}

    def test_builds_cache_from_rows(self):
        import module_7b_requirement_validator as m7b

        tbl = MagicMock()
        tbl.all.return_value = [
            {
                "id": "rec1",
                "fields": {
                    "fldkq8vZZsN6uBTL0": ["recVendorA"],
                    "fldbKVYBi20p6wykZ": ["recClientX"],
                    "fld6Ml1MK3QpmIICK": "Compliant",
                    "fldoTDHEEkekT2jAj": "",
                    "fldWzdPiDQOXqlwhg": "2024-01-01T12:00:00Z",
                },
            },
            {
                "id": "rec2",
                "fields": {
                    "fldkq8vZZsN6uBTL0": ["recVendorB"],
                    "fldbKVYBi20p6wykZ": ["recClientY"],
                    "fld6Ml1MK3QpmIICK": "Non-Compliant",
                    "fldoTDHEEkekT2jAj": "Missing GL coverage",
                    "fldWzdPiDQOXqlwhg": "2024-01-02T08:00:00Z",
                },
            },
        ]

        result = m7b._build_last_log_cache(tbl)

        assert result[("recVendorA", "recClientX")] == {
            "decision": "Compliant",
            "failure_reasons": "",
        }
        assert result[("recVendorB", "recClientY")] == {
            "decision": "Non-Compliant",
            "failure_reasons": "Missing GL coverage",
        }

    def test_later_row_wins_for_same_pair(self):
        """Because rows are sorted ascending by timestamp, the last one in the
        list is the most recent and should overwrite earlier entries."""
        import module_7b_requirement_validator as m7b

        tbl = MagicMock()
        tbl.all.return_value = [
            {
                "id": "rec1",
                "fields": {
                    "fldkq8vZZsN6uBTL0": ["recVendorA"],
                    "fldbKVYBi20p6wykZ": ["recClientX"],
                    "fld6Ml1MK3QpmIICK": "Non-Compliant",
                    "fldoTDHEEkekT2jAj": "Old reason",
                    "fldWzdPiDQOXqlwhg": "2024-01-01T00:00:00Z",
                },
            },
            {
                "id": "rec2",
                "fields": {
                    "fldkq8vZZsN6uBTL0": ["recVendorA"],
                    "fldbKVYBi20p6wykZ": ["recClientX"],
                    "fld6Ml1MK3QpmIICK": "Compliant",
                    "fldoTDHEEkekT2jAj": "",
                    "fldWzdPiDQOXqlwhg": "2024-01-02T00:00:00Z",
                },
            },
        ]

        result = m7b._build_last_log_cache(tbl)
        assert result[("recVendorA", "recClientX")]["decision"] == "Compliant"

    def test_rows_missing_links_are_skipped(self):
        import module_7b_requirement_validator as m7b

        tbl = MagicMock()
        tbl.all.return_value = [
            {
                "id": "rec1",
                "fields": {
                    "fldkq8vZZsN6uBTL0": [],       # empty vendor link
                    "fldbKVYBi20p6wykZ": ["recClientX"],
                    "fld6Ml1MK3QpmIICK": "Compliant",
                    "fldoTDHEEkekT2jAj": "",
                    "fldWzdPiDQOXqlwhg": "2024-01-01T00:00:00Z",
                },
            },
        ]

        result = m7b._build_last_log_cache(tbl)
        assert result == {}

    def test_exception_returns_empty_cache(self):
        import module_7b_requirement_validator as m7b

        tbl = MagicMock()
        tbl.all.side_effect = RuntimeError("network error")

        result = m7b._build_last_log_cache(tbl)
        assert result == {}


# ---------------------------------------------------------------------------
# write_compliance_log — dedup behaviour
# ---------------------------------------------------------------------------

class TestWriteComplianceLogDedup:
    def _call(self, vendor_id, client_id, new_decision, failure_reasons, cache, tbl=None):
        import module_7b_requirement_validator as m7b
        if tbl is None:
            tbl = _make_log_table()
        with patch.dict("sys.modules", {"health_store": MagicMock()}):
            m7b.write_compliance_log(
                vendor_id=vendor_id,
                vendor_name="Test Vendor",
                client_id=client_id,
                client_name="Test Client",
                new_decision=new_decision,
                previous_decision="",
                failure_reasons=failure_reasons,
                compliance_log_table=tbl,
                _last_log_cache=cache,
            )
        return tbl

    def test_no_cache_always_writes(self):
        tbl = _make_log_table()
        import module_7b_requirement_validator as m7b
        with patch.dict("sys.modules", {"health_store": MagicMock()}):
            m7b.write_compliance_log(
                vendor_id="recV1",
                vendor_name="Acme",
                client_id="recC1",
                client_name="Client",
                new_decision="Compliant",
                previous_decision="",
                failure_reasons=[],
                compliance_log_table=tbl,
                _last_log_cache=None,
            )
        assert tbl.create.call_count == 1

    def test_first_occurrence_always_writes(self):
        """Cache exists but pair is new — no prior entry."""
        cache = {}
        tbl = self._call("recV1", "recC1", "Compliant", [], cache)
        assert tbl.create.call_count == 1
        assert cache[("recV1", "recC1")]["decision"] == "Compliant"

    def test_unchanged_decision_and_reasons_skips_write(self):
        cache = {
            ("recV1", "recC1"): {"decision": "Non-Compliant", "failure_reasons": "Missing GL"},
        }
        tbl = self._call("recV1", "recC1", "Non-Compliant", ["Missing GL"], cache)
        assert tbl.create.call_count == 0

    def test_changed_decision_writes(self):
        cache = {
            ("recV1", "recC1"): {"decision": "Non-Compliant", "failure_reasons": ""},
        }
        tbl = self._call("recV1", "recC1", "Compliant", [], cache)
        assert tbl.create.call_count == 1

    def test_changed_failure_reasons_writes(self):
        cache = {
            ("recV1", "recC1"): {"decision": "Non-Compliant", "failure_reasons": "Missing GL"},
        }
        tbl = self._call("recV1", "recC1", "Non-Compliant", ["Missing GL", "Missing WC"], cache)
        assert tbl.create.call_count == 1

    def test_cache_updated_after_write(self):
        """After writing, the cache reflects the new state so a second identical
        call in the same run is correctly suppressed."""
        cache = {}
        tbl = _make_log_table()
        import module_7b_requirement_validator as m7b

        with patch.dict("sys.modules", {"health_store": MagicMock()}):
            for _ in range(3):
                m7b.write_compliance_log(
                    vendor_id="recV1",
                    vendor_name="Acme",
                    client_id="recC1",
                    client_name="Client",
                    new_decision="Compliant",
                    previous_decision="",
                    failure_reasons=[],
                    compliance_log_table=tbl,
                    _last_log_cache=cache,
                )

        # Only the first call should write
        assert tbl.create.call_count == 1

    def test_different_pairs_independent(self):
        """Two different vendor×client pairs are tracked independently."""
        cache = {
            ("recV1", "recC1"): {"decision": "Compliant", "failure_reasons": ""},
        }
        tbl = _make_log_table()
        import module_7b_requirement_validator as m7b

        with patch.dict("sys.modules", {"health_store": MagicMock()}):
            # Same decision for V1/C1 → skip
            m7b.write_compliance_log(
                vendor_id="recV1", vendor_name="Acme",
                client_id="recC1", client_name="Client1",
                new_decision="Compliant", previous_decision="",
                failure_reasons=[], compliance_log_table=tbl,
                _last_log_cache=cache,
            )
            # New pair V2/C2 → write
            m7b.write_compliance_log(
                vendor_id="recV2", vendor_name="Beta",
                client_id="recC2", client_name="Client2",
                new_decision="Compliant", previous_decision="",
                failure_reasons=[], compliance_log_table=tbl,
                _last_log_cache=cache,
            )

        assert tbl.create.call_count == 1

    def test_missing_vendor_id_skips(self):
        tbl = _make_log_table()
        import module_7b_requirement_validator as m7b
        with patch.dict("sys.modules", {"health_store": MagicMock()}):
            m7b.write_compliance_log(
                vendor_id="", vendor_name="Acme",
                client_id="recC1", client_name="Client",
                new_decision="Compliant", previous_decision="",
                failure_reasons=[], compliance_log_table=tbl,
                _last_log_cache={},
            )
        assert tbl.create.call_count == 0

    def test_missing_client_id_skips(self):
        tbl = _make_log_table()
        import module_7b_requirement_validator as m7b
        with patch.dict("sys.modules", {"health_store": MagicMock()}):
            m7b.write_compliance_log(
                vendor_id="recV1", vendor_name="Acme",
                client_id="", client_name="Client",
                new_decision="Compliant", previous_decision="",
                failure_reasons=[], compliance_log_table=tbl,
                _last_log_cache={},
            )
        assert tbl.create.call_count == 0
