"""audit_store.py — SQLite-backed storage for the Audit Workspace.

Payments, payee classifications, and audit files live here, NOT in Airtable.
Airtable gets only one lightweight record per audit.
"""
from __future__ import annotations

import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from typing import Generator

AUDIT_DB_PATH = os.environ.get("AUDIT_DB_PATH", "audit.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS audits (
    id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    client_name TEXT NOT NULL,
    coverage TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    auditor_name TEXT DEFAULT '',
    policy_number TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    due_date TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'Draft',
    airtable_id TEXT DEFAULT '',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_files (
    id TEXT PRIMARY KEY,
    audit_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    file_format TEXT DEFAULT '',
    file_path TEXT NOT NULL,
    row_count INTEGER DEFAULT 0,
    total_amount REAL DEFAULT 0,
    date_min TEXT DEFAULT '',
    date_max TEXT DEFAULT '',
    parse_status TEXT DEFAULT 'pending',
    parse_notes TEXT DEFAULT '',
    uploaded_at REAL NOT NULL,
    FOREIGN KEY (audit_id) REFERENCES audits(id)
);

CREATE TABLE IF NOT EXISTS payments (
    id TEXT PRIMARY KEY,
    audit_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    txn_date TEXT NOT NULL,
    txn_id TEXT DEFAULT '',
    txn_num TEXT DEFAULT '',
    txn_type TEXT DEFAULT '',
    account TEXT DEFAULT '',
    payee_raw TEXT NOT NULL,
    amount REAL NOT NULL,
    is_card INTEGER NOT NULL DEFAULT 0,
    payment_method TEXT DEFAULT '',
    excluded INTEGER NOT NULL DEFAULT 0,
    exclude_reason TEXT DEFAULT '',
    created_at REAL NOT NULL,
    FOREIGN KEY (audit_id) REFERENCES audits(id),
    FOREIGN KEY (file_id) REFERENCES audit_files(id)
);

CREATE TABLE IF NOT EXISTS payee_classifications (
    id TEXT PRIMARY KEY,
    audit_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    payee_raw TEXT NOT NULL,
    vendor_id TEXT DEFAULT '',
    vendor_name TEXT DEFAULT '',
    match_type TEXT DEFAULT '',
    payee_group TEXT NOT NULL DEFAULT 'unclear',
    trade TEXT DEFAULT '',
    is_supplier_only INTEGER DEFAULT 0,
    exclude_reason TEXT DEFAULT '',
    confirmed INTEGER DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    FOREIGN KEY (audit_id) REFERENCES audits(id)
);

CREATE TABLE IF NOT EXISTS payee_aliases (
    id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    payee_raw TEXT NOT NULL,
    vendor_id TEXT NOT NULL,
    vendor_name TEXT NOT NULL,
    confirmed INTEGER DEFAULT 0,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_packs (
    id TEXT PRIMARY KEY,
    audit_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    pack_path TEXT NOT NULL,
    has_open_items INTEGER DEFAULT 0,
    created_at REAL NOT NULL,
    FOREIGN KEY (audit_id) REFERENCES audits(id)
);

CREATE TABLE IF NOT EXISTS audit_remediation (
    id TEXT PRIMARY KEY,
    audit_id TEXT NOT NULL,
    payee_raw TEXT NOT NULL,
    status TEXT DEFAULT '',
    note TEXT DEFAULT '',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    FOREIGN KEY (audit_id) REFERENCES audits(id)
);

CREATE TABLE IF NOT EXISTS audit_emails (
    id TEXT PRIMARY KEY,
    audit_id TEXT NOT NULL,
    email_message_id TEXT NOT NULL,
    email_type TEXT DEFAULT '',
    created_at REAL NOT NULL,
    FOREIGN KEY (audit_id) REFERENCES audits(id)
);

CREATE INDEX IF NOT EXISTS idx_payments_audit ON payments(audit_id);
CREATE INDEX IF NOT EXISTS idx_payments_payee ON payments(audit_id, payee_raw);
CREATE INDEX IF NOT EXISTS idx_payee_class_audit ON payee_classifications(audit_id);
CREATE INDEX IF NOT EXISTS idx_payee_class_client ON payee_classifications(client_id, payee_raw);
CREATE INDEX IF NOT EXISTS idx_payee_aliases_client ON payee_aliases(client_id, payee_raw);
CREATE INDEX IF NOT EXISTS idx_audit_emails_audit ON audit_emails(audit_id);
"""


@contextmanager
def _conn() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(AUDIT_DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with _conn() as conn:
        conn.executescript(_SCHEMA)


# ── Audits ────────────────────────────────────────────────────────────────────

def create_audit(
    client_id: str,
    client_name: str,
    coverage: str,
    start_date: str,
    end_date: str,
    auditor_name: str = "",
    policy_number: str = "",
    notes: str = "",
    due_date: str = "",
) -> dict:
    audit_id = str(uuid.uuid4())
    now = time.time()
    with _conn() as conn:
        conn.execute(
            """INSERT INTO audits
               (id, client_id, client_name, coverage, start_date, end_date,
                auditor_name, policy_number, notes, due_date, status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,'Draft',?,?)""",
            (audit_id, client_id, client_name, coverage, start_date, end_date,
             auditor_name, policy_number, notes, due_date, now, now),
        )
    return get_audit(audit_id)


def get_audit(audit_id: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM audits WHERE id = ?", (audit_id,)
        ).fetchone()
    return dict(row) if row else None


def list_audits(client_id: str | None = None) -> list[dict]:
    with _conn() as conn:
        if client_id:
            rows = conn.execute(
                "SELECT * FROM audits WHERE client_id = ? ORDER BY created_at DESC",
                (client_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM audits ORDER BY created_at DESC"
            ).fetchall()
    return [dict(r) for r in rows]


def update_audit(audit_id: str, **fields) -> None:
    allowed = {
        "client_name", "coverage", "start_date", "end_date",
        "auditor_name", "policy_number", "notes", "due_date", "status", "airtable_id",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    updates["updated_at"] = time.time()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [audit_id]
    with _conn() as conn:
        conn.execute(f"UPDATE audits SET {set_clause} WHERE id = ?", values)


# ── Audit files ───────────────────────────────────────────────────────────────

def create_audit_file(
    audit_id: str,
    original_filename: str,
    file_path: str,
    file_format: str = "",
) -> str:
    file_id = str(uuid.uuid4())
    with _conn() as conn:
        conn.execute(
            """INSERT INTO audit_files
               (id, audit_id, filename, original_filename, file_format, file_path, uploaded_at)
               VALUES (?,?,?,?,?,?,?)""",
            (file_id, audit_id, original_filename, original_filename,
             file_format, file_path, time.time()),
        )
    return file_id


def update_audit_file(file_id: str, **fields) -> None:
    allowed = {
        "file_format", "row_count", "total_amount", "date_min", "date_max",
        "parse_status", "parse_notes",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [file_id]
    with _conn() as conn:
        conn.execute(f"UPDATE audit_files SET {set_clause} WHERE id = ?", values)


def get_audit_files(audit_id: str) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM audit_files WHERE audit_id = ? ORDER BY uploaded_at DESC",
            (audit_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Payments ──────────────────────────────────────────────────────────────────

def insert_payments(rows: list[dict]) -> int:
    if not rows:
        return 0
    now = time.time()
    with _conn() as conn:
        conn.executemany(
            """INSERT OR IGNORE INTO payments
               (id, audit_id, file_id, txn_date, txn_id, txn_num, txn_type,
                account, payee_raw, amount, is_card, payment_method, excluded,
                exclude_reason, created_at)
               VALUES (:id,:audit_id,:file_id,:txn_date,:txn_id,:txn_num,:txn_type,
                       :account,:payee_raw,:amount,:is_card,:payment_method,:excluded,
                       :exclude_reason,:created_at)""",
            [{**r, "id": r.get("id") or str(uuid.uuid4()), "created_at": now} for r in rows],
        )
    return len(rows)


def get_payments(audit_id: str, payee_raw: str | None = None,
                 in_period_only: bool = False,
                 start_date: str | None = None, end_date: str | None = None) -> list[dict]:
    with _conn() as conn:
        q = "SELECT * FROM payments WHERE audit_id = ? AND excluded = 0"
        params: list = [audit_id]
        if payee_raw:
            q += " AND payee_raw = ?"
            params.append(payee_raw)
        if in_period_only and start_date and end_date:
            q += " AND txn_date >= ? AND txn_date <= ?"
            params.extend([start_date, end_date])
        q += " ORDER BY txn_date"
        rows = conn.execute(q, params).fetchall()
    return [dict(r) for r in rows]


def get_payee_summary(audit_id: str, start_date: str, end_date: str) -> list[dict]:
    """Return per-payee payment summary for dates within the audit period."""
    with _conn() as conn:
        rows = conn.execute(
            """SELECT payee_raw,
                      COUNT(*) as txn_count,
                      SUM(amount) as total_amount,
                      MIN(txn_date) as first_date,
                      MAX(txn_date) as last_date
               FROM payments
               WHERE audit_id = ? AND excluded = 0
                 AND txn_date >= ? AND txn_date <= ?
               GROUP BY payee_raw
               ORDER BY total_amount DESC""",
            (audit_id, start_date, end_date),
        ).fetchall()
    return [dict(r) for r in rows]


def get_file_totals(file_id: str) -> dict:
    with _conn() as conn:
        row = conn.execute(
            """SELECT COUNT(*) as count, SUM(amount) as total
               FROM payments WHERE file_id = ?""",
            (file_id,),
        ).fetchone()
    return dict(row) if row else {"count": 0, "total": 0}


# ── Payee classifications ─────────────────────────────────────────────────────

def upsert_payee_classification(
    audit_id: str,
    client_id: str,
    payee_raw: str,
    vendor_id: str = "",
    vendor_name: str = "",
    match_type: str = "",
    payee_group: str = "unclear",
    trade: str = "",
    is_supplier_only: int = 0,
    exclude_reason: str = "",
    confirmed: int = 0,
) -> None:
    now = time.time()
    with _conn() as conn:
        existing = conn.execute(
            "SELECT id FROM payee_classifications WHERE audit_id = ? AND payee_raw = ?",
            (audit_id, payee_raw),
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE payee_classifications
                   SET vendor_id=?, vendor_name=?, match_type=?, payee_group=?,
                       trade=?, is_supplier_only=?, exclude_reason=?, confirmed=?,
                       updated_at=?
                   WHERE audit_id=? AND payee_raw=?""",
                (vendor_id, vendor_name, match_type, payee_group,
                 trade, is_supplier_only, exclude_reason, confirmed, now,
                 audit_id, payee_raw),
            )
        else:
            conn.execute(
                """INSERT INTO payee_classifications
                   (id, audit_id, client_id, payee_raw, vendor_id, vendor_name,
                    match_type, payee_group, trade, is_supplier_only, exclude_reason,
                    confirmed, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (str(uuid.uuid4()), audit_id, client_id, payee_raw,
                 vendor_id, vendor_name, match_type, payee_group,
                 trade, is_supplier_only, exclude_reason, confirmed, now, now),
            )


def get_payee_classifications(audit_id: str) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM payee_classifications WHERE audit_id = ? ORDER BY payee_raw",
            (audit_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_payee_classification(audit_id: str, payee_raw: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM payee_classifications WHERE audit_id = ? AND payee_raw = ?",
            (audit_id, payee_raw),
        ).fetchone()
    return dict(row) if row else None


# ── Payee aliases (remembered across audits per client) ───────────────────────

def get_alias(client_id: str, payee_raw: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM payee_aliases WHERE client_id = ? AND payee_raw = ? AND confirmed = 1",
            (client_id, payee_raw),
        ).fetchone()
    return dict(row) if row else None


def upsert_alias(client_id: str, payee_raw: str, vendor_id: str,
                 vendor_name: str, confirmed: int = 0) -> None:
    with _conn() as conn:
        existing = conn.execute(
            "SELECT id FROM payee_aliases WHERE client_id = ? AND payee_raw = ?",
            (client_id, payee_raw),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE payee_aliases SET vendor_id=?, vendor_name=?, confirmed=? "
                "WHERE client_id=? AND payee_raw=?",
                (vendor_id, vendor_name, confirmed, client_id, payee_raw),
            )
        else:
            conn.execute(
                "INSERT INTO payee_aliases (id, client_id, payee_raw, vendor_id, vendor_name, confirmed, created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), client_id, payee_raw, vendor_id, vendor_name, confirmed, time.time()),
            )


# ── Audit packs ───────────────────────────────────────────────────────────────

def create_audit_pack(audit_id: str, pack_path: str, has_open_items: bool = False) -> dict:
    with _conn() as conn:
        max_ver = conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM audit_packs WHERE audit_id = ?",
            (audit_id,),
        ).fetchone()[0]
        pack_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO audit_packs (id, audit_id, version, pack_path, has_open_items, created_at) "
            "VALUES (?,?,?,?,?,?)",
            (pack_id, audit_id, max_ver + 1, pack_path, int(has_open_items), time.time()),
        )
        row = conn.execute("SELECT * FROM audit_packs WHERE id = ?", (pack_id,)).fetchone()
    return dict(row)


def get_audit_packs(audit_id: str) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM audit_packs WHERE audit_id = ? ORDER BY version DESC",
            (audit_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Remediation ───────────────────────────────────────────────────────────────

def upsert_remediation(audit_id: str, payee_raw: str, status: str, note: str) -> None:
    now = time.time()
    with _conn() as conn:
        existing = conn.execute(
            "SELECT id FROM audit_remediation WHERE audit_id=? AND payee_raw=?",
            (audit_id, payee_raw),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE audit_remediation SET status=?, note=?, updated_at=? WHERE audit_id=? AND payee_raw=?",
                (status, note, now, audit_id, payee_raw),
            )
        else:
            conn.execute(
                "INSERT INTO audit_remediation (id, audit_id, payee_raw, status, note, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), audit_id, payee_raw, status, note, now, now),
            )


def get_remediation(audit_id: str) -> dict[str, dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM audit_remediation WHERE audit_id = ?", (audit_id,)
        ).fetchall()
    return {r["payee_raw"]: dict(r) for r in rows}


# ── Audit emails ──────────────────────────────────────────────────────────────

def record_audit_email(audit_id: str, email_message_id: str, email_type: str = "") -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT INTO audit_emails (id, audit_id, email_message_id, email_type, created_at) VALUES (?,?,?,?,?)",
            (str(uuid.uuid4()), audit_id, email_message_id, email_type, time.time()),
        )


def get_audit_email_ids(audit_id: str) -> list[str]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT email_message_id FROM audit_emails WHERE audit_id = ?", (audit_id,)
        ).fetchall()
    return [r[0] for r in rows]


# ── Auto-init ─────────────────────────────────────────────────────────────────
init_db()
