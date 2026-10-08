"""Pure scoring helpers for the extraction accuracy test (no API calls, no imports of extractor)."""
import re
from datetime import datetime

FIELDS = ("number", "eff", "exp", "basis", "ai", "wos", "pnc")


def family(policy_type) -> str:
    t = (policy_type or "").upper()
    if "EXCESS" in t and "WORKER" in t:
        return "EXCESS_WC"
    if "UMBRELLA" in t:
        return "UMBRELLA"
    if "EXCESS" in t:
        return "EXCESS"
    if "AUTO" in t:
        return "AUTO"
    if "WORKER" in t:
        return "WC"
    if "GENERAL" in t or t.strip() in ("GL", "CGL"):
        return "GL"
    return "OTHER"


def norm_number(v) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(v or "").upper())


def norm_date(v) -> str:
    s = str(v or "").strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def norm_basis(v):
    s = str(v or "").strip().lower().replace("_", "-")
    return s if s in ("occurrence", "claims-made") else None


def _is_empty(p: dict) -> bool:
    return not any(str(p.get(k) or "").strip()
                   for k in ("policy_number", "carrier", "effective_date", "expiration_date", "coverage_limits"))


def score_page(gold: dict, got: dict) -> dict:
    """Compare one page. Returns {'checks': int, 'correct': int, 'errors': [str]}."""
    errors, checks = [], 0

    def check(label, want, have):
        nonlocal checks
        checks += 1
        if want != have:
            errors.append(f"{label}: expected {want!r}, got {have!r}")

    check("document_type", (gold.get("doc_type") or "").lower(), str(got.get("document_type") or "").lower())

    got_policies = [p for p in (got.get("policies") or []) if isinstance(p, dict) and not _is_empty(p)]
    by_family = {}
    for p in got_policies:
        by_family.setdefault(family(p.get("policy_type")), []).append(p)

    check("policy_count", len(gold["policies"]), len(got_policies))

    for gp in gold["policies"]:
        fam = gp["family"]
        candidates = by_family.get(fam) or []
        match = None
        for c in candidates:  # prefer the one with the same number
            if norm_number(c.get("policy_number")) == norm_number(gp["number"]):
                match = c
                break
        match = match or (candidates[0] if candidates else None)
        if match is None:
            for f in FIELDS:
                check(f"{fam}.{f}", gp[f], "<missing policy>")
            continue
        check(f"{fam}.number", norm_number(gp["number"]), norm_number(match.get("policy_number")))
        check(f"{fam}.eff", gp["eff"], norm_date(match.get("effective_date")))
        check(f"{fam}.exp", gp["exp"], norm_date(match.get("expiration_date")))
        check(f"{fam}.basis", gp["basis"], norm_basis(match.get("policy_basis")))
        check(f"{fam}.ai", gp["ai"], bool(match.get("additional_insured_checked")))
        check(f"{fam}.wos", gp["wos"], bool(match.get("waiver_of_subrogation_checked")))
        check(f"{fam}.pnc", gp["pnc"], bool(match.get("primary_noncontributory_checked")))

    return {"checks": checks, "correct": checks - len(errors), "errors": errors}


def summarize(page_scores: dict) -> dict:
    total = sum(s["checks"] for s in page_scores.values())
    right = sum(s["correct"] for s in page_scores.values())
    box = {"basis": [0, 0], "ai": [0, 0], "wos": [0, 0], "pnc": [0, 0]}
    for s in page_scores.values():
        for e in s["errors"]:
            for k in box:
                if f".{k}:" in e:
                    box[k][0] += 1
    return {"checks": total, "correct": right, "accuracy": round(100.0 * right / total, 1) if total else 0.0,
            "pages_perfect": sum(1 for s in page_scores.values() if not s["errors"]),
            "pages": len(page_scores), "checkbox_errors": {k: v[0] for k, v in box.items()}}


def compare_extractions(a: dict, b: dict) -> list:
    """List the differences between two extractions of the same page (a = existing, b = new).
    Uses the same normalization as scoring. Empty rows are ignored."""
    diffs = []
    if str(a.get("document_type") or "").lower() != str(b.get("document_type") or "").lower():
        diffs.append(f"document_type: {a.get('document_type')!r} vs {b.get('document_type')!r}")

    def rows(x):
        out = {}
        for p in (x.get("policies") or []):
            if isinstance(p, dict) and not _is_empty(p):
                out.setdefault(family(p.get("policy_type")), []).append(p)
        return out

    ra, rb = rows(a), rows(b)
    for fam in sorted(set(ra) | set(rb)):
        la, lb = ra.get(fam, []), rb.get(fam, [])
        if len(la) != len(lb):
            diffs.append(f"{fam}: {len(la)} row(s) vs {len(lb)} row(s)")
            continue
        for pa, pb in zip(la, lb):
            pairs = (("number", norm_number(pa.get("policy_number")), norm_number(pb.get("policy_number"))),
                     ("eff", norm_date(pa.get("effective_date")), norm_date(pb.get("effective_date"))),
                     ("exp", norm_date(pa.get("expiration_date")), norm_date(pb.get("expiration_date"))),
                     ("basis", norm_basis(pa.get("policy_basis")), norm_basis(pb.get("policy_basis"))),
                     ("ai", bool(pa.get("additional_insured_checked")), bool(pb.get("additional_insured_checked"))),
                     ("wos", bool(pa.get("waiver_of_subrogation_checked")), bool(pb.get("waiver_of_subrogation_checked"))),
                     ("pnc", bool(pa.get("primary_noncontributory_checked")), bool(pb.get("primary_noncontributory_checked"))))
            for name, x, y in pairs:
                if x != y:
                    diffs.append(f"{fam}.{name}: {x!r} vs {y!r}")
    return diffs
