"""Unit tests for email_monitor._cleanup_old_uploads.

The 24h boundary is split into two tests (just-under preserve, just-over
delete) so the boundary semantics are unambiguous.
"""
import os
import time

import pytest

from email_monitor import _cleanup_old_uploads


def _make_file(path, age_seconds=0):
    """Create a file with mtime set age_seconds in the past."""
    path.write_bytes(b"x")
    mtime = time.time() - age_seconds
    os.utime(path, (mtime, mtime))


def test_cleanup_deletes_files_just_over_24h(tmp_path):
    """File aged 24h + 60s gets deleted."""
    old = tmp_path / "old.pdf"
    _make_file(old, age_seconds=(24 * 60 * 60) + 60)

    deleted = _cleanup_old_uploads(tmp_path)
    assert deleted == 1
    assert not old.exists()


def test_cleanup_preserves_files_just_under_24h(tmp_path):
    """File aged 24h - 60s is preserved."""
    fresh = tmp_path / "fresh.pdf"
    _make_file(fresh, age_seconds=(24 * 60 * 60) - 60)

    deleted = _cleanup_old_uploads(tmp_path)
    assert deleted == 0
    assert fresh.exists()


def test_cleanup_skips_dotfiles(tmp_path):
    """.gitkeep and .DS_Store are preserved regardless of age."""
    gitkeep = tmp_path / ".gitkeep"
    dsstore = tmp_path / ".DS_Store"
    _make_file(gitkeep, age_seconds=999 * 24 * 60 * 60)
    _make_file(dsstore, age_seconds=999 * 24 * 60 * 60)

    deleted = _cleanup_old_uploads(tmp_path)
    assert deleted == 0
    assert gitkeep.exists()
    assert dsstore.exists()


def test_cleanup_skips_subdirectories(tmp_path):
    """Subdirs aren't traversed or deleted."""
    subdir = tmp_path / "nested"
    subdir.mkdir()
    nested_file = subdir / "old.pdf"
    _make_file(nested_file, age_seconds=999 * 24 * 60 * 60)

    deleted = _cleanup_old_uploads(tmp_path)
    assert deleted == 0
    assert subdir.exists()
    assert nested_file.exists()


def test_cleanup_handles_permission_error_gracefully(tmp_path, monkeypatch):
    """If unlink() raises OSError, function logs and continues with other files."""
    bad = tmp_path / "bad.pdf"
    good = tmp_path / "good.pdf"
    _make_file(bad, age_seconds=999 * 24 * 60 * 60)
    _make_file(good, age_seconds=999 * 24 * 60 * 60)

    original_unlink = type(bad).unlink

    def maybe_fail_unlink(self, *args, **kwargs):
        if self.name == "bad.pdf":
            raise PermissionError("simulated")
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(type(bad), "unlink", maybe_fail_unlink)

    deleted = _cleanup_old_uploads(tmp_path)
    assert deleted == 1  # good.pdf got deleted
    assert bad.exists()  # bad.pdf preserved due to PermissionError
    assert not good.exists()


def test_cleanup_no_op_when_uploads_dir_empty(tmp_path):
    """Empty directory → zero deletions, no error."""
    deleted = _cleanup_old_uploads(tmp_path)
    assert deleted == 0


def test_cleanup_no_op_when_uploads_dir_missing(tmp_path):
    """Nonexistent directory → zero deletions, no error."""
    missing = tmp_path / "does_not_exist"
    deleted = _cleanup_old_uploads(missing)
    assert deleted == 0
