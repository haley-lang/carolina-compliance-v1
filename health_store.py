"""health_store.py — SQLite health event store + write-rate circuit breaker.

No Airtable dependency — survives Airtable being full or down.
"""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Generator

# ── Config ────────────────────────────────────────────────────────────────────

CB_MAX_WRITES = int(os.environ.get("CB_MAX_WRITES", "200"))
CB_WINDOW_MINUTES = int(os.environ.get("CB_WINDOW_MINUTES", "10"))

VALID_CATEGORIES = frozenset({
    "write_error", "api_error", "circuit_breaker", "pipeline",
    "heartbeat", "canary", "email_intake", "write_rate", "manual_action",
})
VALID_LEVELS = frozenset({"ok", "warn", "error"})
VALID_SERVICES = frozenset({"pulse_cron", "pipeline", "email_monitor", "canary", "run_pipeline"})

# ── DB path ───────────────────────────────────────────────────────────────────

def get_db_path() -> str:
    """Return the resolved path to the SQLite database file."""
    return os.environ.get("HEALTH_DB_PATH", "health.db")


# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS health_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL    NOT NULL,
    category   TEXT    NOT NULL,
    level      TEXT    NOT NULL CHECK (level IN ('ok', 'warn', 'error')),
    title      TEXT    NOT NULL,
    body       TEXT    NOT NULL DEFAULT '',
    table_name TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS write_tally (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL    NOT NULL,
    table_name TEXT    NOT NULL,
    count      INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS heartbeats (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      REAL    NOT NULL,
    service TEXT    NOT NULL
);
"""


@contextmanager
def _get_conn() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(get_db_path(), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create tables if they do not already exist. Called on import."""
    with _get_conn() as conn:
        conn.executescript(_SCHEMA)


# ── Public API ────────────────────────────────────────────────────────────────

def record_event(
    category: str,
    level: str,
    title: str,
    body: str = "",
    table_name: str = "",
) -> int:
    """Insert a health event row and return its new row id."""
    if category not in VALID_CATEGORIES:
        category = "pipeline"  # safe fallback, don't raise on monitoring calls
    if level not in VALID_LEVELS:
        level = "warn"
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO health_events (ts, category, level, title, body, table_name) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (time.time(), category, level, title, body, table_name),
        )
        return cur.lastrowid


def record_heartbeat(service: str) -> None:
    """Record a heartbeat ping for the named service."""
    with _get_conn() as conn:
        conn.execute(
            "INSERT INTO heartbeats (ts, service) VALUES (?, ?)",
            (time.time(), service),
        )


def record_write(table_name: str, count: int = 1) -> None:
    """Log a write operation and trigger circuit breaker check if threshold crossed."""
    with _get_conn() as conn:
        conn.execute(
            "INSERT INTO write_tally (ts, table_name, count) VALUES (?, ?, ?)",
            (time.time(), table_name, count),
        )
    # Check after inserting so the new write is included in the rate.
    if not check_circuit_breaker(table_name):
        rate = get_write_rate(table_name, window_minutes=CB_WINDOW_MINUTES)
        record_event(
            "circuit_breaker",
            "error",
            f"Circuit breaker tripped for {table_name}",
            body=(
                f"{rate} writes to '{table_name}' in the last "
                f"{CB_WINDOW_MINUTES} minutes (limit {CB_MAX_WRITES})"
            ),
            table_name=table_name,
        )


def check_circuit_breaker(table_name: str) -> bool:
    """Return True if write rate is within limits, False if tripped."""
    rate = get_write_rate(table_name, window_minutes=CB_WINDOW_MINUTES)
    return rate <= CB_MAX_WRITES


def get_recent_events(limit: int = 100, since_hours: float = 24) -> list[dict]:
    """Return up to `limit` events from the last `since_hours` hours, newest first."""
    cutoff = time.time() - since_hours * 3600
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT id, ts, category, level, title, body, table_name "
            "FROM health_events "
            "WHERE ts >= ? "
            "ORDER BY ts DESC "
            "LIMIT ?",
            (cutoff, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def get_write_rate(table_name: str, window_minutes: int = 10) -> int:
    """Return total write count for `table_name` within the last `window_minutes`."""
    cutoff = time.time() - window_minutes * 60
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(count), 0) AS total "
            "FROM write_tally "
            "WHERE table_name = ? AND ts >= ?",
            (table_name, cutoff),
        ).fetchone()
    return int(row["total"]) if row else 0


def get_last_heartbeat(service: str) -> float | None:
    """Return the Unix timestamp of the most recent heartbeat for `service`, or None."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT ts FROM heartbeats WHERE service = ? ORDER BY ts DESC LIMIT 1",
            (service,),
        ).fetchone()
    return row["ts"] if row else None


def prune_old_data(keep_days: int = 7) -> None:
    """Delete rows older than `keep_days` days from all three tables."""
    cutoff = time.time() - keep_days * 86400
    with _get_conn() as conn:
        conn.execute("DELETE FROM health_events WHERE ts < ?", (cutoff,))
        conn.execute("DELETE FROM write_tally WHERE ts < ?", (cutoff,))
        conn.execute("DELETE FROM heartbeats WHERE ts < ?", (cutoff,))


# ── Auto-init on import ───────────────────────────────────────────────────────
init_db()
