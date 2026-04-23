# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Carolina Compliance Solutions — a Certificate of Insurance (COI) compliance engine for contractors. The system ingests COI PDFs/images (via email, inbound webhook, or portal upload), uses Anthropic Claude to extract structured insurance data, matches it to vendors/clients in Airtable, evaluates compliance against each client's insurance requirements, and drives vendor follow-up via queued email.

All persistent state lives in **Airtable**. There is no application database. `notifications.db` (SQLite) is only used by the webhook service for local dedup.

## Running things

The project has two virtual environments and it matters which one you use:

- `.venv/` — the one `run_pipeline.py` and `daily_cron.py` expect (they hardcode `.venv/bin/python`).
- `venv/` — the legacy one `run_pipeline.sh` uses. Prefer `.venv`.

```bash
# Full pipeline (what Railway cron runs daily)
.venv/bin/python run_pipeline.py

# Manually reprocess a triage record by Airtable record id
.venv/bin/python run_pipeline.py --reprocess rec123ABC

# Web service (SendGrid inbound parse + Stripe webhook)
gunicorn module_16_inbound_webhook:app --bind 0.0.0.0:$PORT --workers 2

# Daily cron tasks only (escalations, reprocess, exception expiry)
.venv/bin/python daily_cron.py

# Weekly Airtable backup
.venv/bin/python airtable_backup.py
```

**Config is validated on startup.** `config.validate_config()` rejects the process unless:
- `EMAIL_ADDRESS` equals `coi-intake@carolinacompliancesolutions.com` exactly (no aliases allowed for IMAP login)
- `EMAIL_PASSWORD` is a 16-character Gmail App Password (spaces are stripped)
- `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID` are set

If you see `EnvironmentError: Missing required environment variables` or the alias/app-password check failing, fix `.env` before doing anything else.

## Tests

```bash
# Pytest suite (mocks Airtable + Anthropic — never hits real APIs)
.venv/bin/python -m pytest tests/

# A single test
.venv/bin/python -m pytest tests/test_edge_date_formats.py -v
.venv/bin/python -m pytest tests/test_edge_date_formats.py::TestClass::test_name

# Full edge-case suite with HTML + JUnit XML reports
.venv/bin/python run_edge_case_tests.py
.venv/bin/python run_edge_case_tests.py -k date   # filter by keyword
# Output: tests/edge_case_report.html, tests/edge_case_results.xml

# Legacy module tests at the repo root (not in tests/)
.venv/bin/python -m pytest test_bulk_vendor_import.py test_expiration_dedup.py \
  test_module_15_email_queue_builder.py test_incoming_extraction_matcher_slice.py
```

`tests/conftest.py` auto-patches env vars and mocks `pyairtable.Api` and `anthropic.Anthropic` — tests should never need real credentials. `tests/_bootstrap.py` stubs packages that may be missing in the test env.

## Pipeline architecture

`run_pipeline.py` is the orchestrator. It runs each module as a **subprocess** (not an import), so each module re-reads config and reconnects to Airtable. Modules communicate only through Airtable state — there is no in-memory hand-off.

Execution order (from `run_pipeline.py`):

1. `email_monitor.py` — IMAP intake, attachments → `uploads/`, rows → **Incoming Documents**
2. `extractor.py` — each unprocessed file in `uploads/` → Claude (via `call_claude_with_retry`, retries 500/529) → JSON in `extracted/`
3. `airtable_importer.py` — `extracted/*.json` → **Incoming Extractions** table
4. `processor.py` — match Named Insured to vendor → **Insurance Policies** + **Insurance Certificates**; unmatched extractions go to `Needs Review`
5. `module_8_policy_expiration_monitor.py` — set Expiration Status at 90/60/30/7-day thresholds. Uses `Last Reminder Threshold` field so each threshold fires at most once per policy.
6. `module_8b.py` — cancellation / endorsement / reinstatement handling for already-processed policies
7. `module_7b_requirement_validator.py` — evaluate each vendor's policies against their **Client Requirements**; writes compliance status + failure reasons
8. `module_17_queue_initial_requests.py` → `module_18_vendor_initial_request_sender.py` — queue and send first-contact COI requests
9. `module_19_requirements_followup.py` — follow-ups when requirements are missing
10. `module_15_email_queue_builder.py` — builds reminder/deficiency emails in **Email Queue**
11. `module_10_vendor_email_sender.py` — drains **Email Queue** via SendGrid
12. `daily_cron.py` — auto-escalations (Pending Internal Review > 48h), exception expiry, triage reprocess

**Explicitly disabled in V1** (kept in the tree, not wired into `run_pipeline.py`): `module_7a_client_setup_wizard.py`, `module_6_task_creator.py`, `module_11_task_generator.py`, `task_generator.py`. Don't add them back without reason.

### Two entry points, one pipeline

- **Cron path** — `run_pipeline.py` runs daily at 12:00 UTC (see `railway.toml`). Batch-processes everything since the last run.
- **Webhook path** — `module_16_inbound_webhook.py` (Flask, Gunicorn) receives SendGrid Inbound Parse. During **business hours (Mon–Fri 8am–6pm ET)** it runs the pipeline immediately on the incoming PDF via subprocesses and passes the target file via `COI_TARGET_FILE` env var. Outside business hours, the PDF is queued for the next cron run.

The same webhook also handles Stripe payment webhooks (new-client signup → Airtable client creation).

## Airtable as the source of truth

- **`airtable_constants.py`** holds the canonical table names, field names, and status values. Import from here rather than hardcoding strings. Some modules additionally hardcode **field IDs** (`fldXXX...`) and **table IDs** (`tblXXX...`) directly — this is intentional because field renames don't break ID-based lookups. When you see a raw `fld...` / `tbl...`, it's a deliberate ID reference, not a magic string to replace.
- Key tables: `Vendors`, `Clients`, `Client Requirements`, `Vendor Client Assignments`, `Insurance Policies`, `Insurance Certificates`, `Incoming Documents`, `Incoming Extractions`, `Email Queue`, `Compliance Log`, `Vendor Requirement Overrides`.
- Compliance failure reasons are written to the **Compliance Log** table by `module_7b_requirement_validator.py` and read back by `module_15_email_queue_builder.py` to personalize deficiency emails.
- The pipeline is idempotent by design: modules dedupe on policy number, sent-status, `Last Reminder Threshold`, etc. Re-running the pipeline should not create duplicate policies or re-send emails.

## Extraction contract (Claude output)

`extractor.py` prompts Claude to **never guess** — missing fields come back as `null` or `[]`. Downstream modules must handle missing fields rather than trusting defaults. The full JSON from Claude is stored in **Incoming Extractions → Raw JSON** and re-parsed by `processor.py` — treat that field as the canonical extraction record.

Claude API calls go through `call_claude_with_retry` in `extractor.py`, which retries only on 500 and 529 (overloaded). Other errors propagate immediately — do not widen the retry net.

## Deployment

`railway.toml` defines the Railway service:
- Web service runs the Gunicorn webhook on `$PORT` with health check at `/health`.
- Daily pipeline cron runs `python run_pipeline.py` at `0 12 * * *` UTC.
- Weekly backup cron runs `python airtable_backup.py` at `0 6 * * 0` UTC.

`Procfile` duplicates the web service definition for non-Railway hosts.

## Background constraints (from `SUPPORT_SCALING_REQUIREMENTS.md`)

Product targets under 10 minutes of support time per client per month. This shapes design choices: self-serve only, no per-client custom logic, explicit statuses (no implicit inference), automation default-on. If you're about to add a manual-review step or client-specific branch, that's a product smell — push it back to explicit data in Airtable instead.
