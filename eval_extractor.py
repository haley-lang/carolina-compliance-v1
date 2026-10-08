"""
Extraction accuracy test: runs the certificate pages in a gold folder through
chosen model(s) and prompt version(s) and scores them against tests/gold/rtr_gold.json.

Run from the project folder (needs ANTHROPIC_API_KEY in .env):

  .venv/bin/python eval_extractor.py "<gold folder with f1_01.pdf ...>" --dry-run
  .venv/bin/python eval_extractor.py "<gold folder>" --models claude-opus-4-5 --prompts current,checkbox_v2

Writes eval_results/<timestamp>/ (one JSON per page+model+prompt, plus summary.md).
Nothing in the pipeline or Airtable is touched.
"""
import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import eval_scoring

GOLD_PATH = Path(__file__).parent / "tests" / "gold" / "rtr_gold.json"

# $ per million tokens (input, output) — checked against Anthropic pricing on 2026-10-07; update if it changes.
PRICES = {
    "claude-opus-4-5": (5.0, 25.0),
    "claude-opus-5-5": (4.0, 20.0),
    "claude-sonnet-5-5": (2.0, 10.0),
    "claude-haiku-4-5-20251001": (1.0, 5.0),
}
DEFAULT_MODELS = "claude-opus-4-5,claude-opus-5-5,claude-sonnet-5-5"

def prompt_variants(base: str) -> dict:
    """Return the two prompt variants used during eval.

    "checkbox_v2" is base unchanged — CHECKBOX_RULES are already embedded in
    extractor.SYSTEM_PROMPT (the source of truth).  "current" strips them so
    we can compare against old extractions made without those rules.
    """
    import extractor as _ext  # late import — consistent with this file's pattern
    current = base.replace(_ext.CHECKBOX_RULES, "", 1)
    return {"current": current, "checkbox_v2": base}


def estimate_cost(model: str, in_tok: int, out_tok: int) -> float:
    pin, pout = PRICES.get(model, (0.0, 0.0))
    return (in_tok * pin + out_tok * pout) / 1_000_000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("gold_dir")
    ap.add_argument("--models", default=DEFAULT_MODELS)
    ap.add_argument("--prompts", default="current,checkbox_v2")
    ap.add_argument("--dry-run", action="store_true", help="list the calls and a cost estimate; no API calls")
    ap.add_argument("--runs", type=int, default=1, help="repeat each call N times (shows run-to-run variation)")
    args = ap.parse_args()

    gold = json.loads(GOLD_PATH.read_text())["pages"]
    gold_dir = Path(args.gold_dir).expanduser()
    models = [m.strip() for m in args.models.split(",") if m.strip()]
    prompt_names = [p.strip() for p in args.prompts.split(",") if p.strip()]
    missing = [pid for pid in gold if not (gold_dir / f"{pid}.pdf").exists()]
    if missing:
        sys.exit(f"Missing PDFs in {gold_dir}: {missing}")

    calls = len(gold) * len(models) * len(prompt_names) * args.runs
    # rough per-call size: one page image + prompt in, ~1.2k tokens out
    est = sum(estimate_cost(m, 3500, 1200) for m in models) * len(gold) * len(prompt_names) * args.runs
    print(f"{len(gold)} pages x {len(models)} model(s) x {len(prompt_names)} prompt(s) x {args.runs} run(s) = {calls} calls")
    print(f"Rough cost estimate: ${est:.2f} (an estimate; real usage is printed at the end)")
    if args.dry_run:
        return

    import anthropic
    import extractor  # uses the same prompt, image conversion and JSON parsing as the pipeline
    if not extractor.ANTHROPIC_API_KEY:
        sys.exit("ANTHROPIC_API_KEY is not set in .env")
    client = anthropic.Anthropic(api_key=extractor.ANTHROPIC_API_KEY)
    variants = prompt_variants(extractor.SYSTEM_PROMPT)

    out_dir = Path("eval_results") / datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    results, spend = {}, {}
    for model in models:
        for pname in prompt_names:
            key = f"{model} | {pname}"
            results[key], spend[key] = {}, 0.0
            for pid, g in gold.items():
                for run in range(args.runs):
                    content = extractor.build_message_content(gold_dir / f"{pid}.pdf")
                    t0 = time.time()
                    try:
                        resp = client.messages.create(model=model, system=variants[pname],
                                                      messages=[{"role": "user", "content": content}], max_tokens=6000)
                        # Some models return a "thinking" block first; only the text blocks hold the answer.
                        raw = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text").strip()
                        data = extractor._parse_json_response(raw)
                        data = extractor.normalize_policy_dates(data)
                        spend[key] += estimate_cost(model, resp.usage.input_tokens, resp.usage.output_tokens)
                    except Exception as exc:  # keep going; a failed call counts as a miss
                        print(f"  FAILED {key} {pid}: {exc}")
                        data = {}
                    sc = eval_scoring.score_page(g, data)
                    results[key][f"{pid}#{run + 1}"] = sc
                    (out_dir / f"{model}__{pname}__{pid}__{run + 1}.json").write_text(json.dumps(data, indent=2))
                    print(f"  {key} {pid}: {sc['correct']}/{sc['checks']} ({time.time() - t0:.1f}s)")

    lines = ["# Extraction accuracy test", "", f"Pages: {len(gold)}  Runs per page: {args.runs}", "",
             "| Model | Prompt | Accuracy | Perfect pages | Checkbox errors (basis/ai/wos/pnc) | Cost |",
             "|---|---|---|---|---|---|"]
    for key, page_scores in results.items():
        s = eval_scoring.summarize(page_scores)
        model, pname = key.split(" | ")
        cb = s["checkbox_errors"]
        lines.append(f"| {model} | {pname} | {s['accuracy']}% | {s['pages_perfect']}/{s['pages']} | "
                     f"{cb['basis']}/{cb['ai']}/{cb['wos']}/{cb['pnc']} | ${spend[key]:.2f} |")
    lines += ["", "## Errors", ""]
    for key, page_scores in results.items():
        lines.append(f"### {key}")
        for pid, sc in page_scores.items():
            for e in sc["errors"]:
                lines.append(f"- {pid}: {e}")
        lines.append("")
    (out_dir / "summary.md").write_text("\n".join(lines))
    print("\n".join(lines[:12]))
    print(f"\nFull report: {out_dir / 'summary.md'}")


if __name__ == "__main__":
    main()
