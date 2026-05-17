"""Unit tests for config.COI_REVIEW_CONFIDENCE_THRESHOLD env var handling."""
import importlib
import os

import pytest


def _reimport_config():
    """Re-import config to re-evaluate the threshold expression."""
    import config
    importlib.reload(config)
    return config


def test_threshold_default_is_0_95(monkeypatch):
    """With env var unset, default is 0.95."""
    monkeypatch.delenv("COI_REVIEW_CONFIDENCE_THRESHOLD", raising=False)
    cfg = _reimport_config()
    assert cfg.COI_REVIEW_CONFIDENCE_THRESHOLD == 0.95


def test_threshold_override_via_env_var(monkeypatch):
    """Env var override changes the threshold value."""
    monkeypatch.setenv("COI_REVIEW_CONFIDENCE_THRESHOLD", "0.85")
    cfg = _reimport_config()
    assert cfg.COI_REVIEW_CONFIDENCE_THRESHOLD == 0.85


def test_threshold_malformed_raises_at_import(monkeypatch):
    """Malformed env var → ValueError at config import (fail-loud)."""
    monkeypatch.setenv("COI_REVIEW_CONFIDENCE_THRESHOLD", "not_a_number")
    with pytest.raises(ValueError):
        _reimport_config()
    # Restore to default for any subsequent tests in this run
    monkeypatch.delenv("COI_REVIEW_CONFIDENCE_THRESHOLD", raising=False)
    _reimport_config()
