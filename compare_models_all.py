"""
Re-reads EVERY split certificate page of a client with a chosen model and lists where it
disagrees with the extraction already stored in Airtable. Disagreements are the pages worth
checking by eye. Nothing in Airtable is changed.

  .venv/bin/python compare_models_all.py "<folder with the original bundle PDFs>" --model claude-sonnet-5-5 --dry-run
  .venv/bin/python compare_models_all.py "<folder with the original bundle PDFs>" --model claude-sonnet-5-5 --prompt checkbox_v2

Writes eval_results/compare_<model>_<time>/disagreements.md and one JSON per page.
"""
import argparse
import json
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import eval_scoring
from eval_extractor import PRICES, estimate_cost, prompt_variants


def norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", Path(name).stem.lower()).strip("_")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("folder")
    ap.add_argument("--model", default="claude-sonnet-5-5")
    ap.add_argument("--prompt", default="checkbox_v2")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    import config
    import pdf_bundle
    from pyairtable import Api
    from airtable_importer import INCOMING_EXTRACTIONS_TABLE, clean_base_id

    table = Api(config.AIRTABLE_API_KEY.strip()).table(clean_base_id(config.AIRTABLE_BASE_ID), INCOMING_EXTRACTIONS_TABLE)
    stored = {}
    for rec in table.all():
        f = rec["fields"]
        name, raw = f.get("Source Filename"), f.get("Raw JSON")
        if name and raw and f.get("Review Status") != "Rejected":
            try:
                stored[norm(name)] = (name, json.loads(raw))
            except ValueError:
                pass

    with tempfile.TemporaryDirectory() as tmp:
        pages = {}
        for pdf in sorted(Path(args.folder).expanduser().glob("*")):
            if pdf.suffix.lower() != ".pdf":
                continue
            out = Path(tmp) / norm(pdf.name)
            out.mkdir(parents=True, exist_ok=True)
            for part in pdf_bundle.split_pdf(pdf, out) or []:
                pages[norm(Path(part).name)] = Path(part)
        todo = {k: v for k, v in pages.items() if k in stored}
        est = len(todo) * estimate_cost(args.model, 3500, 1200)
        print(f"{len(todo)} pages match stored rows. Rough cost: ${est:.2f}")
        if args.dry_run:
            return

        import anthropic
        import extractor
        client = anthropic.Anthropic(api_key=extractor.ANTHROPIC_API_KEY)
        system = prompt_variants(extractor.SYSTEM_PROMPT)[args.prompt]
        out_dir = Path("eval_results") / f"compare_{args.model}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        out_dir.mkdir(parents=True, exist_ok=True)
        lines, agree, spend = [], 0, 0.0
        for key, path in todo.items():
            name, old = stored[key]
            try:
                resp = client.messages.create(model=args.model, system=system, max_tokens=6000,
                                              messages=[{"role": "user", "content": extractor.build_message_content(path)}])
                raw = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text").strip()
                new = extractor.normalize_policy_dates(extractor._parse_json_response(raw))
                spend += estimate_cost(args.model, resp.usage.input_tokens, resp.usage.output_tokens)
            except Exception as exc:
                print(f"  FAILED {name}: {exc}")
                lines.append(f"## {name}\n- call failed: {exc}\n")
                continue
            (out_dir / f"{key}.json").write_text(json.dumps(new, indent=2))
            diffs = eval_scoring.compare_extractions(old, new)
            if diffs:
                lines.append(f"## {name}\n" + "\n".join(f"- {d}" for d in diffs) + "\n")
                print(f"  {name}: {len(diffs)} difference(s)")
            else:
                agree += 1
                print(f"  {name}: same")
        header = f"# {args.model} / {args.prompt} vs stored extractions\n\nSame: {agree} of {len(todo)}. Cost: ${spend:.2f}\n\n"
        (out_dir / "disagreements.md").write_text(header + "\n".join(lines))
        print(header)
        print(f"Report: {out_dir / 'disagreements.md'}")


if __name__ == "__main__":
    main()
