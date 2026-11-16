"""Tests for the payee_library table in audit_store."""
import os
import tempfile
import pytest


@pytest.fixture()
def tmp_db(monkeypatch):
    """Return a fresh in-memory audit_store pointing at a temp DB."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    monkeypatch.setenv("AUDIT_DB_PATH", db_path)

    # Re-import with the env var set so the module picks up the new path
    import importlib
    import audit_store as _store
    importlib.reload(_store)
    yield _store
    os.unlink(db_path)


# ── save_to_library / get_library_suggestion ─────────────────────────────────

def test_save_and_retrieve_client_specific(tmp_db):
    tmp_db.save_to_library("client1", "Home Depot", "supplier", confirmed=1)
    result = tmp_db.get_library_suggestion("client1", "Home Depot")
    assert result is not None
    assert result["suggested_group"] == "supplier"


def test_client_suggestion_overrides_shared(tmp_db):
    # Shared entry says "not_sub"
    tmp_db.save_to_library(None, "Acme Corp", "not_sub", confirmed=1)
    # Client-specific says "likely_sub"
    tmp_db.save_to_library("client1", "Acme Corp", "likely_sub", confirmed=1)

    result = tmp_db.get_library_suggestion("client1", "Acme Corp")
    assert result["suggested_group"] == "likely_sub"


def test_shared_suggestion_returned_when_no_client_entry(tmp_db):
    tmp_db.save_to_library(None, "Big Box Store", "supplier", confirmed=1)
    result = tmp_db.get_library_suggestion("client_xyz", "Big Box Store")
    assert result is not None
    assert result["suggested_group"] == "supplier"


def test_no_match_returns_none(tmp_db):
    result = tmp_db.get_library_suggestion("client1", "Nonexistent LLC")
    assert result is None


def test_normalized_lookup_ignores_case_and_punctuation(tmp_db):
    tmp_db.save_to_library("client1", "Home Depot #42", "supplier", confirmed=1)
    # Lookup with different casing / punctuation should still match
    result = tmp_db.get_library_suggestion("client1", "HOME DEPOT #42")
    assert result is not None
    assert result["suggested_group"] == "supplier"


def test_upsert_updates_existing_entry(tmp_db):
    tmp_db.save_to_library("client1", "Alpha LLC", "unclear", confirmed=0)
    tmp_db.save_to_library("client1", "Alpha LLC", "ccs_vendor", confirmed=1)
    result = tmp_db.get_library_suggestion("client1", "Alpha LLC")
    assert result["suggested_group"] == "ccs_vendor"
    assert result["confirmed"] == 1


def test_empty_payee_name_returns_none(tmp_db):
    result = tmp_db.get_library_suggestion("client1", "")
    assert result is None


# ── seed_starter_list ─────────────────────────────────────────────────────────

def test_starter_list_seeded(tmp_db):
    # Starter list seeds Home Depot as supplier
    result = tmp_db.get_library_suggestion("any_client", "Home Depot")
    assert result is not None
    assert result["suggested_group"] == "supplier"


def test_starter_list_seed_idempotent(tmp_db):
    # Calling seed_starter_list() again should not raise or duplicate
    tmp_db.seed_starter_list()
    tmp_db.seed_starter_list()
    result = tmp_db.get_library_suggestion("any_client", "lowes")
    assert result is not None


def test_starter_list_client_override_beats_starter(tmp_db):
    # Client marks "home depot" as something else
    tmp_db.save_to_library("client1", "home depot", "likely_sub", confirmed=1)
    result = tmp_db.get_library_suggestion("client1", "home depot")
    assert result["suggested_group"] == "likely_sub"
