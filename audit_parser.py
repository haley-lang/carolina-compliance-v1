"""audit_parser.py — Parse payment file formats for the Audit Workspace.

Formats:
  1. QuickBooks Online Transaction List CSV (3 title rows, then data headers)
  2. Generic CSV / XLSX with column mapping
  3. QuickBooks Expenses by Vendor Summary PDF (future)
"""
from __future__ import annotations

import csv
import io
import re
from datetime import date, datetime, timedelta
from typing import Any


# ── Date parsing ──────────────────────────────────────────────────────────────

_DATE_FMTS = (
    "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d",
    "%d-%b-%Y", "%b %d, %Y", "%B %d, %Y",
    "%m-%d-%Y",
)


def _parse_date(s: str) -> date | None:
    s = (s or "").strip()
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_amount(s: str) -> float | None:
    cleaned = re.sub(r"[^0-9.\-]", "", str(s or ""))
    try:
        return float(cleaned)
    except ValueError:
        return None


# ── Account classification ────────────────────────────────────────────────────

_CARD_KEYWORDS = (
    "amex", "american express", "credit", "visa", "mastercard",
    "discover", "lowes", "home depot card", "sam's club card",
)
_BANK_KEYWORDS = ("checking", "savings", "bank", "ozark", "bof a", "cash")


def _is_card_account(account: str) -> bool:
    al = (account or "").lower()
    if any(k in al for k in _CARD_KEYWORDS):
        return True
    return False


# ── QuickBooks Transaction List CSV ──────────────────────────────────────────

_QB_TITLE_ROW_SIGNALS = frozenset({
    "transaction list", "transaction list by vendor",
    "expense transactions", "transactions by vendor",
})

_QB_HEADER_SIGNALS = frozenset({
    "transaction type", "transaction id", "txn date",
})


def _looks_like_qb_header(row: list[str]) -> bool:
    normed = {c.lower().strip() for c in row}
    return bool(normed & _QB_HEADER_SIGNALS)


def _detect_qb_csv(lines: list[str]) -> bool:
    """Return True if this looks like a QuickBooks Transaction List CSV."""
    for line in lines[:6]:
        if any(sig in line.lower() for sig in _QB_TITLE_ROW_SIGNALS):
            return True
    return False


def parse_qb_transaction_list_csv(content: str) -> dict:
    """Parse a QuickBooks Online Transaction List CSV.

    Returns:
        {
            "format": "qb_transaction_list",
            "row_count": int,
            "total_amount": float,
            "date_min": str,
            "date_max": str,
            "transactions": [{txn_date, txn_id, txn_num, txn_type, account,
                               payee_raw, amount, is_card, payment_method}, ...],
            "account_counts": {account_name: count},
            "warnings": [str],
        }
    """
    lines = content.splitlines()
    warnings = []

    # Find header row: skip title rows until we see column headers
    header_idx = None
    for i, line in enumerate(lines[:10]):
        row = next(csv.reader([line]))
        if _looks_like_qb_header(row):
            header_idx = i
            break

    if header_idx is None:
        # Fallback: skip the first 3 rows (as briefed) and use row 4 as headers
        header_idx = 3

    # Parse all rows from header_idx onward, deduplicating column names
    data_lines_list = lines[header_idx:]
    if not data_lines_list:
        return {"format": "qb_transaction_list", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["No data rows found"]}

    # Read raw headers and deduplicate (Amount appears twice in QB format)
    raw_headers = next(csv.reader([data_lines_list[0]]))
    seen: dict[str, int] = {}
    deduped_headers: list[str] = []
    for h in raw_headers:
        hkey = h.strip()
        if hkey in seen:
            seen[hkey] += 1
            deduped_headers.append(f"{hkey}__{seen[hkey]}")
        else:
            seen[hkey] = 0
            deduped_headers.append(hkey)

    # Re-read as DictReader with deduped headers
    data_body = "\n".join(data_lines_list[1:])  # skip header row
    reader = csv.DictReader(io.StringIO(data_body), fieldnames=deduped_headers)
    all_rows = list(reader)

    if not all_rows:
        return {"format": "qb_transaction_list", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["No data rows found"]}

    # Build a header map (case-insensitive, strip whitespace, first occurrence wins)
    sample_keys = deduped_headers
    lower_keys: dict[str, str] = {}
    for k in sample_keys:
        kl = k.lower().split("__")[0].strip()  # strip the __N dedup suffix for lookup
        if kl not in lower_keys:
            lower_keys[kl] = k

    def _col(*names: str) -> str | None:
        for n in names:
            if n.lower() in lower_keys:
                return lower_keys[n.lower()]
        return None

    date_col    = _col("date", "txn date", "transaction date")
    txnid_col   = _col("transaction id", "txn id", "num")
    num_col     = _col("num", "ref no")
    type_col    = _col("transaction type", "type", "txn type")
    account_col = _col("account", "bank account", "credit card account")
    payee_col   = _col("vendor", "payee", "name", "payee name", "vendor name")
    method_col  = _col("payment method")

    # Amount: may appear twice — first occurrence is usually the signed amount
    amount_key = _col("amount")

    transactions = []
    account_counts: dict[str, int] = {}
    skipped = 0

    for row in all_rows:
        txn_date_raw = (row.get(date_col) or "").strip() if date_col else ""
        payee_raw    = (row.get(payee_col) or "").strip() if payee_col else ""
        amount_raw   = (row.get(amount_key) or "").strip() if amount_key else ""
        account      = (row.get(account_col) or "").strip() if account_col else ""
        txn_type     = (row.get(type_col) or "").strip() if type_col else ""
        txn_id       = (row.get(txnid_col) or "").strip() if txnid_col else ""
        txn_num      = (row.get(num_col) or "").strip() if num_col else ""
        pay_method   = (row.get(method_col) or "").strip() if method_col else ""

        # Skip blank or summary rows
        if not payee_raw and not amount_raw:
            skipped += 1
            continue
        if payee_raw.lower() in ("total", "", "grand total"):
            skipped += 1
            continue

        txn_date = _parse_date(txn_date_raw)
        if not txn_date:
            skipped += 1
            continue

        amt = _parse_amount(amount_raw)
        if amt is None:
            skipped += 1
            continue

        is_card = _is_card_account(account)
        # Normalize: bank transactions are negative in QB → take abs
        # Card transactions are positive → keep
        amount_pos = abs(amt)

        account_counts[account] = account_counts.get(account, 0) + 1

        transactions.append({
            "txn_date":     txn_date.isoformat(),
            "txn_id":       txn_id,
            "txn_num":      txn_num,
            "txn_type":     txn_type,
            "account":      account,
            "payee_raw":    payee_raw,
            "amount":       amount_pos,
            "is_card":      1 if is_card else 0,
            "payment_method": pay_method,
        })

    if not transactions:
        return {"format": "qb_transaction_list", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["Could not parse any transactions — check column headers"]}

    dates = [t["txn_date"] for t in transactions]
    total = sum(t["amount"] for t in transactions)

    if skipped:
        warnings.append(f"Skipped {skipped} blank or summary rows")

    return {
        "format":        "qb_transaction_list",
        "row_count":     len(transactions),
        "total_amount":  round(total, 2),
        "date_min":      min(dates),
        "date_max":      max(dates),
        "transactions":  transactions,
        "account_counts": account_counts,
        "warnings":      warnings,
    }


# ── Generic CSV / XLSX ────────────────────────────────────────────────────────

def parse_generic_csv(content: str, col_map: dict | None = None) -> dict:
    """Parse a generic CSV. col_map = {date: col_name, payee: col_name, amount: col_name}."""
    reader = csv.DictReader(io.StringIO(content))
    all_rows = list(reader)
    if not all_rows:
        return {"format": "generic_csv", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["Empty CSV"]}

    sample_keys = list(all_rows[0].keys())
    lower_keys  = {k.lower().strip(): k for k in sample_keys}

    def _col(*names: str) -> str | None:
        for n in names:
            nl = n.lower().strip()
            if nl in lower_keys:
                return lower_keys[nl]
        return None

    if col_map:
        date_col   = col_map.get("date")
        payee_col  = col_map.get("payee")
        amount_col = col_map.get("amount")
    else:
        date_col   = _col("date", "txn date", "transaction date", "posted date")
        payee_col  = _col("payee", "name", "vendor", "description", "payee name")
        amount_col = _col("amount", "debit", "credit", "total", "payment amount")

    transactions = []
    warnings = []
    skipped = 0

    for row in all_rows:
        txn_date_raw = (row.get(date_col) or "").strip() if date_col else ""
        payee_raw    = (row.get(payee_col) or "").strip() if payee_col else ""
        amount_raw   = (row.get(amount_col) or "").strip() if amount_col else ""

        if not payee_raw and not amount_raw:
            skipped += 1
            continue

        txn_date = _parse_date(txn_date_raw)
        if not txn_date:
            skipped += 1
            continue

        amt = _parse_amount(amount_raw)
        if amt is None:
            skipped += 1
            continue

        amount_pos = abs(amt)
        transactions.append({
            "txn_date":     txn_date.isoformat(),
            "txn_id":       "",
            "txn_num":      "",
            "txn_type":     "Expense",
            "account":      "",
            "payee_raw":    payee_raw,
            "amount":       amount_pos,
            "is_card":      0,
            "payment_method": "",
        })

    if not transactions:
        return {"format": "generic_csv", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["No parseable rows found"]}

    dates  = [t["txn_date"] for t in transactions]
    total  = sum(t["amount"] for t in transactions)

    if not date_col:
        warnings.append("Date column not found — mapped by position")
    if not payee_col:
        warnings.append("Payee column not found — mapped by position")

    return {
        "format":        "generic_csv",
        "row_count":     len(transactions),
        "total_amount":  round(total, 2),
        "date_min":      min(dates),
        "date_max":      max(dates),
        "transactions":  transactions,
        "account_counts": {},
        "warnings":      warnings,
    }


def parse_generic_xlsx(file_bytes: bytes, col_map: dict | None = None) -> dict:
    """Parse an XLSX file using openpyxl."""
    try:
        import openpyxl
    except ImportError:
        return {"format": "generic_xlsx", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["openpyxl not installed; cannot parse XLSX"]}

    try:
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
    except Exception as exc:
        return {"format": "generic_xlsx", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": [f"Could not open XLSX: {exc}"]}

    if not rows:
        return {"format": "generic_xlsx", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["Empty workbook"]}

    # Find header row
    header_row_idx = 0
    headers = []
    for i, row in enumerate(rows[:10]):
        row_strs = [str(c or "").lower().strip() for c in row]
        if any(sig in row_strs for sig in ("date", "payee", "amount", "vendor")):
            header_row_idx = i
            headers = [str(c or "").strip() for c in row]
            break

    if not headers:
        return {"format": "generic_xlsx", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["No header row found in XLSX"]}

    lower_headers = {h.lower(): i for i, h in enumerate(headers)}

    def _idx(*names: str) -> int | None:
        for n in names:
            if n.lower() in lower_headers:
                return lower_headers[n.lower()]
        return None

    if col_map:
        date_idx   = lower_headers.get((col_map.get("date") or "").lower())
        payee_idx  = lower_headers.get((col_map.get("payee") or "").lower())
        amount_idx = lower_headers.get((col_map.get("amount") or "").lower())
    else:
        date_idx   = _idx("date", "txn date", "transaction date")
        payee_idx  = _idx("payee", "name", "vendor", "description")
        amount_idx = _idx("amount", "debit", "credit", "total")

    transactions = []
    for row in rows[header_row_idx + 1:]:
        def _cell(idx):
            if idx is None or idx >= len(row):
                return ""
            v = row[idx]
            if v is None:
                return ""
            if hasattr(v, "isoformat"):  # date/datetime object
                return v.isoformat()[:10]
            return str(v).strip()

        txn_date_raw = _cell(date_idx)
        payee_raw    = _cell(payee_idx)
        amount_raw   = _cell(amount_idx)

        if not payee_raw and not amount_raw:
            continue

        txn_date = _parse_date(txn_date_raw) or (
            datetime.fromisoformat(txn_date_raw[:10]).date()
            if len(txn_date_raw) >= 10 and txn_date_raw[4] == "-"
            else None
        )
        if not txn_date:
            continue

        amt = _parse_amount(amount_raw)
        if amt is None:
            continue

        transactions.append({
            "txn_date":     txn_date.isoformat(),
            "txn_id":       "",
            "txn_num":      "",
            "txn_type":     "Expense",
            "account":      "",
            "payee_raw":    payee_raw,
            "amount":       abs(amt),
            "is_card":      0,
            "payment_method": "",
        })

    if not transactions:
        return {"format": "generic_xlsx", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["No parseable rows"]}

    dates = [t["txn_date"] for t in transactions]
    total = sum(t["amount"] for t in transactions)

    return {
        "format":        "generic_xlsx",
        "row_count":     len(transactions),
        "total_amount":  round(total, 2),
        "date_min":      min(dates),
        "date_max":      max(dates),
        "transactions":  transactions,
        "account_counts": {},
        "warnings":      [],
    }


# ── Dispatcher ────────────────────────────────────────────────────────────────

def parse_payment_file(filename: str, file_bytes: bytes,
                       col_map: dict | None = None) -> dict:
    """Auto-detect format and parse a payment file.

    Returns the standard dict with keys:
        format, row_count, total_amount, date_min, date_max,
        transactions, account_counts, warnings
    """
    ext = (filename or "").lower().rsplit(".", 1)[-1]

    if ext in ("xlsx", "xls"):
        result = parse_generic_xlsx(file_bytes, col_map=col_map)
        return result

    # CSV / PDF path
    try:
        content = file_bytes.decode("utf-8-sig", errors="replace")
    except Exception:
        content = file_bytes.decode("latin-1", errors="replace")

    if ext == "csv":
        lines = content.splitlines()
        if _detect_qb_csv(lines):
            return parse_qb_transaction_list_csv(content)
        # Check if it has the QB header row pattern even without title rows
        for line in lines[:6]:
            row = next(csv.reader([line]))
            if _looks_like_qb_header(row):
                return parse_qb_transaction_list_csv(content)
        return parse_generic_csv(content, col_map=col_map)

    if ext == "pdf":
        return _parse_pdf_expense_summary(file_bytes)

    # Unknown extension — try QB CSV first, then generic
    lines = content.splitlines()
    if _detect_qb_csv(lines):
        return parse_qb_transaction_list_csv(content)
    return parse_generic_csv(content, col_map=col_map)


# ── QB Expenses by Vendor Summary PDF ────────────────────────────────────────

def _parse_pdf_expense_summary(file_bytes: bytes) -> dict:
    """Parse a QuickBooks Expenses by Vendor Summary PDF.

    This format has vendors as rows and dates as columns.
    Returns synthetic "daily total" transactions per vendor.
    """
    try:
        import pdfplumber
    except ImportError:
        return {"format": "qb_expense_summary_pdf", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": ["pdfplumber not installed"]}

    warnings: list[str] = []
    transactions: list[dict] = []
    total_check: dict[str, float] = {}  # vendor → stated total
    computed_totals: dict[str, float] = {}

    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            for page_num, page in enumerate(pdf.pages):
                table = page.extract_table()
                if not table or len(table) < 2:
                    continue

                # Find column positions: first row is headers
                headers = [str(c or "").strip() for c in table[0]]

                # Identify date columns vs vendor/total columns
                date_cols: list[tuple[int, date]] = []
                total_col: int | None = None
                vendor_col: int = 0

                for i, h in enumerate(headers):
                    if h.lower() in ("", "vendor", "name"):
                        vendor_col = i
                        continue
                    if h.lower() in ("total", "grand total"):
                        total_col = i
                        continue
                    d = _parse_date(h)
                    if d:
                        date_cols.append((i, d))

                if not date_cols:
                    warnings.append(f"Page {page_num + 1}: no date columns found")
                    continue

                for row in table[1:]:
                    if not row or len(row) <= vendor_col:
                        continue
                    vendor = str(row[vendor_col] or "").strip()
                    if not vendor or vendor.lower() in ("total", "grand total", ""):
                        continue

                    row_total = 0.0
                    for col_idx, col_date in date_cols:
                        if col_idx >= len(row):
                            continue
                        amt = _parse_amount(str(row[col_idx] or ""))
                        if amt is None:
                            continue
                        amt_pos = abs(amt)
                        if amt_pos == 0:
                            continue
                        row_total += amt_pos
                        transactions.append({
                            "txn_date":     col_date.isoformat(),
                            "txn_id":       "",
                            "txn_num":      "",
                            "txn_type":     "Expense",
                            "account":      "Expense Summary",
                            "payee_raw":    vendor,
                            "amount":       amt_pos,
                            "is_card":      0,
                            "payment_method": "",
                        })

                    if total_col and total_col < len(row):
                        stated = _parse_amount(str(row[total_col] or ""))
                        if stated is not None:
                            total_check[vendor] = abs(stated)
                            computed_totals[vendor] = round(
                                computed_totals.get(vendor, 0) + row_total, 2
                            )

    except Exception as exc:
        return {"format": "qb_expense_summary_pdf", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": [f"PDF parse error: {exc}"]}

    # Verify row totals
    mismatch_vendors = []
    for vendor, stated in total_check.items():
        computed = round(computed_totals.get(vendor, 0), 2)
        if abs(computed - stated) > 0.02:
            mismatch_vendors.append(f"{vendor} (stated {stated:.2f}, computed {computed:.2f})")

    if mismatch_vendors:
        warnings.append(
            "Row total mismatch (flagged, not dropped): " + "; ".join(mismatch_vendors)
        )

    if not transactions:
        return {"format": "qb_expense_summary_pdf", "row_count": 0, "total_amount": 0,
                "date_min": "", "date_max": "", "transactions": [],
                "account_counts": {}, "warnings": warnings or ["No transactions parsed from PDF"]}

    dates = [t["txn_date"] for t in transactions]
    total = sum(t["amount"] for t in transactions)
    warnings.insert(0, "This format has daily totals only, not individual transactions. Less detailed than a Transaction List.")

    return {
        "format":        "qb_expense_summary_pdf",
        "row_count":     len(transactions),
        "total_amount":  round(total, 2),
        "date_min":      min(dates),
        "date_max":      max(dates),
        "transactions":  transactions,
        "account_counts": {},
        "warnings":      warnings,
    }


# ── Coverage gap check ────────────────────────────────────────────────────────

def check_file_covers_period(
    file_date_min: str, file_date_max: str,
    audit_start: str, audit_end: str,
) -> list[str]:
    """Return a list of warning strings if the file doesn't cover the full audit period."""
    warnings = []
    fd_min = _parse_date(file_date_min)
    fd_max = _parse_date(file_date_max)
    a_start = _parse_date(audit_start)
    a_end   = _parse_date(audit_end)

    if not all([fd_min, fd_max, a_start, a_end]):
        return warnings

    if fd_min > a_start:
        warnings.append(
            f"This file starts {fd_min.strftime('%m/%d/%Y')}, so payments "
            f"from {a_start.strftime('%m/%d/%Y')} to "
            f"{(fd_min - timedelta(days=1)).strftime('%m/%d/%Y')} "
            f"are not included."
        )

    if fd_max < a_end:
        warnings.append(
            f"This file ends {fd_max.strftime('%m/%d/%Y')}, so payments "
            f"from {(fd_max + timedelta(days=1)).strftime('%m/%d/%Y')} "
            f"to {a_end.strftime('%m/%d/%Y')} are not included."
        )

    return warnings
