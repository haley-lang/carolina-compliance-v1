# Carolina Compliance Solutions — System SOP

**Audited:** 2026-08-07/08, directly against the code in this repository (main branch, clean working tree) and the live Airtable base ("Carolina Compliance Solutions OS", `appCGgww0Pt7KE04u`) via the Airtable API. Nothing in this document is from memory or documentation alone — every claim is either a direct code citation (`file.py:line`) or explicitly marked **UNVERIFIED** where the code doesn't settle the question (e.g. anything that lives in the Railway UI, Stripe dashboard, Gmail forwarding rules, or Softr's own configuration, none of which are in this repo).

This document supersedes `README.md` and `AI_SYSTEM_CONTEXT.md` where they conflict with the code — both are confirmed stale (see §9). `CLAUDE.md` is largely accurate but has one significant drift, also flagged in §9 (the documented "webhook path" for inbound mail no longer exists).

---

## 1. System Overview

Carolina Compliance Solutions (CCS) is a subscription service that chases subcontractors' Certificates of Insurance (COIs) on behalf of general-contractor clients. A vendor (subcontractor) emails or is emailed for a COI; Anthropic Claude extracts the structured policy data from the PDF/image; the system matches the document to a known vendor and client, evaluates the extracted policies against that client's insurance requirements, and — if something's missing or expired — automatically emails the vendor (and, on a fixed escalation ladder, the vendor's insurance agent, then finally surfaces the case to Haley for a manual GC-facing email). All persistent state lives in Airtable; there is no application database except a local SQLite file (`notifications.db`) used only for webhook dedup, and this audit found no code path that actually reads or writes it (see §9).

**Where things run**, confirmed from `railway.toml`'s comment block (itself **not enforced by Railway** — see the caveat at the top of §3) and `Procfile`:
- One Railway **web service** — `gunicorn module_16_inbound_webhook:app` — a Flask app that handles Stripe billing webhooks and a Stripe Checkout redirect endpoint. It does **not** handle inbound COI email (see §2 and §9 for why this contradicts `CLAUDE.md`).
- Four Railway **cron services**, each running a standalone script: `run_pipeline.py` (the main daily pipeline), `daily_cron.py` (escalations/exceptions/reprocessing), `railway_cron_followup.py` (advances the vendor follow-up ladder), and `airtable_backup.py` (weekly CSV export to Cloudflare R2).
- **Airtable** is the single source of truth for every table (Vendors, Clients, Insurance Policies, Insurance Certificates, Email Queue, Compliance Log, etc. — full inventory in §7).
- **Softr** (`app.carolinacompliancesolutions.com`) is the client-facing portal. It is not part of this repository — evidence in the code strongly suggests it reads Airtable directly via Softr's native connector, with no API layer of this repo's in between (see §6).
- External services: Anthropic Claude (extraction + email classification fallback), SendGrid (all outbound email), Cloudflare R2 (PDF storage), Stripe (billing), Gmail/Google Workspace (IMAP mailbox + forwarding — configuration not in this repo).

---

## 2. Inbound Pipeline (COI Intake)

### The event-driven-vs-schedule-driven answer, up front

**The entire intake-through-certificate pipeline is schedule-driven, not event-driven, in the code as it exists today.** There is no webhook, queue, or trigger that fires processing when a vendor's email actually arrives. Everything from "email lands in the inbox" through "certificate compliance is evaluated" runs once per invocation of `run_pipeline.py`, which — per the `railway.toml` comment block — is a single daily cron (`0 12 * * *` UTC, i.e. roughly 7–8am ET depending on DST). **This means a vendor's COI reply sent at 9am ET on a Tuesday is not looked at until the next scheduled run, up to ~24 hours later.**

This directly contradicts `CLAUDE.md`'s "Two entry points, one pipeline" section, which describes a webhook path that "receives SendGrid Inbound Parse" and "runs the pipeline immediately on the incoming PDF via subprocesses" during business hours, using a `COI_TARGET_FILE` env var. **That code does not exist in this repo.** `module_16_inbound_webhook.py`'s own docstring says so explicitly: *"The previous SendGrid Inbound Parse route was removed in favor of that path [IMAP polling]"* (`module_16_inbound_webhook.py:19-21`). Confirmed by grep: there is no `/webhook/sendgrid` or `/webhook/inbound` route anywhere, and `COI_TARGET_FILE` appears nowhere in the codebase. `module_16`'s only three routes are `/health`, `/api/create-checkout-session`, and `/webhook/stripe-payment` (`module_16_inbound_webhook.py:129,148,175`). **Treat `CLAUDE.md`'s webhook-intake description as stale; this SOP's schedule-driven description is what the code actually does.**

The one genuinely event-driven piece in the whole system is the follow-up ladder's response detection — an inbound email that matches an active ladder's named insured halts that ladder immediately when the *next* `email_monitor.py` run processes it (still gated by the same once-daily cadence, not truly real-time).

### Stage-by-stage trace

**Stage 0 — mail arrives.** A vendor sends to `coi@carolinacompliancesolutions.com` (the address given out in outbound emails as `config.INBOUND_EMAIL`, `config.py:21`). The mailbox actually polled via IMAP is a *different* address, `coi-intake@carolinacompliancesolutions.com` (`config.py:11`, hard-enforced by `config.validate_config()` at `config.py:64-68` — the process refuses to start if `EMAIL_ADDRESS` isn't exactly this). **UNVERIFIED: how mail addressed to `coi@` reaches the `coi-intake@` mailbox.** This is presumably a Google Workspace alias or forwarding rule, but that configuration lives outside this repo and isn't confirmable from code.

**Stage 1 — `email_monitor.py` (Module 1).**
- **Trigger:** subprocess call, `run_pipeline.py:27`, first of twelve steps in the pipeline's fixed sequence. Per the `railway.toml` reference comment, this whole sequence runs once daily. Nothing else invokes `email_monitor.py`.
- **What it does:** connects via IMAP (`imapclient`), searches `INBOX` for `UNSEEN` messages using `BODY.PEEK[]` (not `RFC822`, specifically to avoid marking a message Seen before the Airtable write is confirmed — `email_monitor.py:186-190`). For each message: decodes sender/subject/date/CC/Message-ID/body-snippet, saves attachments whose extension is in `config.ALLOWED_EXTENSIONS` (`.pdf .png .jpg .jpeg .tiff .tif`, `config.py:39`) to `uploads/`, creates one row in **Incoming Documents** per email (`Sender Email`, `Subject`, `Date Received`, `File Names`, `Status`, `Source Email CC`, `Source Email Message ID`, `Source Email Body Snippet` — `email_monitor.py:319-324`, written via `airtable_client.create_document_record`), best-effort uploads each attachment to R2 and patches back a `PDF R2 Key` (`email_monitor.py:365-399` — failure here is non-fatal and logged, not retried automatically), then marks the message `\Seen` and moves it to a Gmail "Processed" label (`email_monitor.py:407-430`, each step independently wrapped so a label-move failure doesn't unwind the Seen flag or the Airtable write).
- Runs email classification (`email_classifier.classify_email`) — rule-based pass first, LLM fallback (Claude Haiku) second. **If classification fails for any reason (including a missing `ANTHROPIC_API_KEY`), it silently defaults to "treat as a new COI, proceed to extraction"** (`email_classifier.py:321-322,378-380`) — the system fails *open* toward automated processing here, the opposite direction from the confidence-based review gate used later in the pipeline.
- Runs several handlers depending on classification: bounce/auto-reply handling (with a Vendors lookup to flag the vendor), cloud-link detection, no-attachment handling, and per-attachment gating (`gate_unsupported_type`, `gate_oversize_attachment`).
- **Success/failure recorded:** `Incoming Documents.Status` (`Pending Review`, `Failed`, `Oversize`, `Unsupported Format`, etc.), plus `Event Type`, `Classification Confidence`, `Classification Method`, `Skip Reason`.
- **Confirmed gap — gating is cosmetic, not a real block.** `gate_oversize_attachment` and `gate_unsupported_type` (`operational_email_handler.py:411-440, 451-480`) only update the `Incoming Documents` Airtable record — **neither one deletes, moves, or otherwise touches the file already sitting in `uploads/`** (it was written to disk earlier, at `email_monitor.py:246`, before any gating runs). `extractor.py` (the very next pipeline stage, same run) has **no size check at all** and scans `uploads/` directly by file extension, independent of anything `email_monitor.py` decided. A `.pdf` flagged "Oversize" in Airtable will still be sent to Claude for extraction on the same run. The only attachments genuinely blocked from extraction are `.tiff`/`.tif` files, because `extractor.py`'s own supported-extension set (`{.pdf,.jpg,.jpeg,.png}`, `extractor.py:54`) is narrower than the save-to-disk filter (`config.ALLOWED_EXTENSIONS` includes `.tiff/.tif`) — those files get correctly flagged in Airtable but then sit in `uploads/` untouched until the 24-hour age-based cleanup (`email_monitor.py:33-62`) deletes them on a later run. **The `actionable_results`/`passing_attachments` in-memory lists built during this gating (`email_monitor.py:315,565,575`) are computed and then discarded when the script exits — nothing downstream ever reads them.**

**Stage 2 — `extractor.py` (Module 2).**
- **Trigger:** subprocess, `run_pipeline.py:28`, immediately after Stage 1 in the same run.
- **What it does:** lists every file in `uploads/` matching `{.pdf,.jpg,.jpeg,.png}` that doesn't already have a same-stem JSON in `extracted/` (`extractor.py:248-301` — this file-existence check is the entire dedup mechanism; re-running is safe). For PDFs, first classifies pages to find an ACORD 25 certificate page; if none is found, writes a "triage stub" JSON with `confidence: 0.0` and a `_triage_reason`, **without ever calling Claude** (`extractor.py:754-769`). Otherwise sends the document to Claude (model `claude-opus-4-5`, not documented anywhere outside the code — `extractor.py:606,620,689`) and writes the resulting JSON to `extracted/`.
- **Retry behavior — confirmed matches CLAUDE.md:** `call_claude_with_retry` retries only on `anthropic.APIStatusError` with `status_code in (500, 529)`, up to 3 attempts with a 10-second sleep between (`extractor.py:23-45`). Every other error type or status code propagates on the first occurrence.
- **Success/failure:** never written to Airtable or any persistent status — only logged to stdout and counted in an end-of-run `succeeded`/`failed` tally (`extractor.py:817-828`). A file that fails extraction just stays in `uploads/` and is retried automatically on the next scheduled run (once daily) — **if nobody reads the pipeline logs, a stuck file gives no visible signal in Airtable.**

**Stage 3 — `airtable_importer.py` (Module 3).**
- **Trigger:** subprocess, `run_pipeline.py:29`, same run.
- **What it does:** reads `extracted/*.json`, dedupes against existing **Incoming Extractions** records by `Source Filename` (`airtable_importer.py:59-91`) — **if that Airtable read itself fails, dedup silently disables and the batch will attempt to re-import everything** (`airtable_importer.py:76-80`). Runs a separate content-based duplicate check (`operational_email_handler.check_duplicate_extraction`, `operational_email_handler.py:302-359`): same Named Insured + same sorted policy numbers + same sorted expiration dates within a 30-day window. This check is wrapped in a broad `try/except` that fails **open** — if the duplicate-check query itself errors, the record is treated as not-a-duplicate and proceeds (`airtable_importer.py:264-268`, a documented tradeoff).
- Writes one **Incoming Extractions** row per file: `Source Filename`, `Document Type`, `Named Insured`, `Certificate Holder`, `Contact Emails`, `Policies Count`, `Raw JSON` (this is the canonical extraction record per `CLAUDE.md`), `Extraction Processed At`, `Processing Status`, `Confidence Score`, `Review Status`, `Review Reason`.
- **Review routing (`review_gate.compute_review_status`, precedence order):** `Possible Duplicate` beats `Low Confidence` beats auto-approval. Extraction confidence below `COI_REVIEW_CONFIDENCE_THRESHOLD` (env-overridable, default **0.95**, `config.py:45-47`) routes to `Processing Status = "Low Confidence"`, `Review Status = "Pending Review"`, `Review Reason = "Low Confidence"`. Possible duplicate routes to `Processing Status = "Duplicate"`. Everything else lands `Processing Status = "Imported"` and proceeds automatically.
- **Success/failure:** per-file failures are logged and counted, not persisted anywhere — a failed import means the file stays in `extracted/` and is retried next run (single-file CLI mode has **no dedup at all**, only the batch loop does).

**Stage 4 — `processor.py` (Module 4).**
- **Trigger:** subprocess, `run_pipeline.py:30`, same run.
- **Which records it pulls:** `Processing Status = 'Imported'` OR (`'Low Confidence'` AND `Review Reason = 'Low Confidence'`) (`processor.py:772-804`). Low Confidence records run through vendor matching and the 1E onboarding-window gate only — downstream policy/certificate writes are skipped until manual approval. Records with `Processing Status = 'Duplicate'` or `'Needs Review'` are excluded — they sit until a human acts.
- **Vendor matching** (`incoming_extraction_matcher_slice.py`, called from `processor.py:1784-1789`): exact match on `Vendor Name`/`Aliases` → exact match on `DBA Names` → fuzzy match (`rapidfuzz.fuzz.ratio`) with **auto-match ≥85, human-review-queue 60–84 ("Pending Match"), no match <60** (`incoming_extraction_matcher_slice.py:69-70`) → broker-email match (confidence 90) → `MATCH_STATUS_UNMATCHED`. Client resolution is separate, by Certificate Holder text against `Clients.Certificate Holder` with a `Client Name` fallback. Note this fuzzy-match confidence (0–100, rapidfuzz score) is a completely different number from the Claude extraction confidence (0.0–1.0) used in Stage 3 — don't conflate the two when reading Airtable.
- **On match:** creates/updates **Insurance Policies** (deduped by exact `Policy Number` match; a newer-dated renewal of the same vendor/type marks the prior policy `Superseded`), creates an **Insurance Certificates** record (this is the moment a document becomes an Insurance Certificates row), runs `policy_timing_evaluator` and `midterm_change_detector` (both non-fatal, wrapped in try/except), then calls `module_7b_requirement_validator.evaluate_assignment` **inline** and writes the outcome to `Insurance Certificates.Compliance Status` (`processor.py:2183-2369`). **UNVERIFIED** whether this inline call also appends a **Compliance Log** row (that write lives in `module_7b_requirement_validator.write_compliance_log`, and it's not confirmed whether `processor.py`'s inline call path reaches it) — see §9 for why this matters (the standalone module_7b pipeline stage, three steps later, re-evaluates every vendor×client pair again regardless).
- **Terminal `Processing Status` values, more granular than "Needs Review" alone:** `"Processed"` (success), `"Needs Review"` (invalid JSON, blank/colliding Named Insured, ambiguous alias, invalid policy number), **`"Unmatched"`** (matcher found no vendor — a distinct status from `"Needs Review"`; any tooling that filters only on `"Needs Review"` will miss these), `"Duplicate"` (every incoming policy already existed, nothing new created).
- **Confirmed gap — no certificate-creation dedup guard.** `create_certificate` is called with no prior lookup by source filename or extraction ID. If an uncaught exception happens in the certificate/policy-creation steps (not the individually-guarded timing/midterm/compliance sub-steps) *after* the certificate is created but before `Processing Status` is advanced to `"Processed"`, the extraction is picked up again on the next run and a **second Insurance Certificates row gets created for the same document.**

**Stage 5 — `module_8_policy_expiration_monitor.py`.** Trigger: `run_pipeline.py:31`. Pure date-threshold state machine over **Insurance Policies**: sets `Expiration Status` to `Expired`, `Expiring in 7/30/60/90 Days`, `Active`, `Pending Active` (future-effective), or `Missing Expiration Date`, tightest threshold first. Writes are guarded (skip if unchanged) — idempotent. It does **not** write `Last Reminder Threshold` itself; that field is only updated by `module_15_email_queue_builder.py` after a reminder email is actually queued (see §4).

**Stage 6 — `module_8b.py`.** Trigger: `run_pipeline.py:32`. Scans **Incoming Extractions** for `cancellation_notice`/`endorsement`/`reinstatement` document types not yet actioned (`Cancellation Action Taken` unchecked — this flag is the idempotency guard). Classifies cancellation subtype by regex, sets vendor/assignment `Compliance Status = "Has Open Items"` on full/premium-finance cancellations, queues up to three internal alert emails.

**Stage 7 — `module_7b_requirement_validator.py` (standalone run).** Trigger: `run_pipeline.py:33`. Re-evaluates **every** active `Vendor Client Assignments` row against `Client Requirements` (not just the one record `processor.py` just touched), rolls the worst-status-across-all-lines up to the vendor record, appends one **Compliance Log** row per vendor×client pair on **every single run regardless of whether the decision changed** (unbounded growth, no dedup — flagged in §9), and fires/halts the follow-up ladder via post-evaluation hooks. Full comparison-rule detail is in §4/§9.

At this point a document has reached its final resting state in **Insurance Certificates**, with a `Compliance Status` that determines whether §4's outbound ladder fires.

---

## 3. All Cron Jobs / Services Inventory

**Caveat that applies to every schedule below:** `railway.toml` is explicit that it is "intentionally minimal" and that "service-specific settings (startCommand, healthcheckPath, cronSchedule) are configured per-service in the Railway UI, not here" (`railway.toml:3-7`). Everything below labeled "per railway.toml comment" is a **documentation comment describing intent**, not an enforced value — it could have drifted from what's actually configured in the Railway dashboard. **Verify the live schedules in the Railway UI before relying on any time given here.**

| Service | Purpose | Trigger | Entry point | Reads/writes | Depends on |
|---|---|---|---|---|---|
| **carolina-compliance-v1** (web) | Stripe billing webhooks + Checkout redirect; health check | HTTP, always-on | `gunicorn module_16_inbound_webhook:app --bind 0.0.0.0:$PORT --workers 2` (`Procfile:1`) | **Clients** table (create/update on Stripe events) | Stripe, SendGrid, Airtable |
| **cron-pipeline** | Full daily intake→compliance pipeline | Cron, `0 12 * * *` UTC per comment (`railway.toml:16`) | `python run_pipeline.py` | Everything in §2 + §4, plus runs `daily_cron.py` as its own last step (`run_pipeline.py:54-57`) | IMAP, Anthropic, Airtable, R2, SendGrid |
| **cron-daily** | Cancellation-effective-date escalation, exception expiry, 48h triage auto-escalation, reprocess reviewed triage records | Cron, `30 12 * * *` UTC per comment (`railway.toml:17`) — **30 minutes after cron-pipeline** | `python daily_cron.py` | Incoming Extractions, Email Queue, Clients, Vendors, Vendor Requirement Overrides | Airtable |
| **cron-followup** | Advances the vendor follow-up ladder (Day 2 / Day 4 / Day 6) | Cron, `0 13 * * 1-5` UTC per comment (`railway.toml:18`, weekdays only) | `python railway_cron_followup.py` | Outbound Follow-Ups, Email Queue | Airtable — calls into `module_22_followup_ladder.advance_ladder` |
| **cron-backup** | Weekly export of 11 core tables to CSV, uploaded to R2 | Cron, `0 6 * * 0` UTC per comment (`railway.toml:19`, Sundays) | `python airtable_backup.py` | Read-only from Clients, Vendors, Insurance Policies, Insurance Certificates, Incoming Extractions, Client Requirements, Vendor Client Assignments, Email Queue, Compliance Log, Outbound Follow-Ups, Tasks; writes to R2 | Airtable, R2 |

**Confirmed redundancy worth verifying in the Railway UI:** `run_pipeline.py` already runs `daily_cron.py` as its own final step (`run_pipeline.py:54-57`), and **`daily_cron.py` is also scheduled as its own separate cron service 30 minutes later.** If both are actually live as documented, the four `daily_cron.py` tasks (cancellation monitoring, exception expiry, triage auto-escalation, reprocess) run **twice** every day. Each of those four functions has its own idempotency guard (status-field flips that stop matching on the second pass), so a double-run is very likely harmless — just wasteful and worth confirming isn't accidental.

**DST is not handled.** `railway_cron_followup.py`'s own docstring flags this directly: *"Railway cron runs in UTC — see railway.toml for the translation. DST IS NOT AUTO-HANDLED by Railway cron"* (`railway_cron_followup.py:8-9`). The `13:00 UTC` cron time only lands at 8:00am ET during Eastern Standard Time; during Eastern Daylight Time it fires at 9:00am ET instead. Same one-hour drift applies to `cron-pipeline`'s and `cron-daily`'s UTC times against their intended ET-morning target.

**NO SCHEDULE CONFIGURED (by design):** none of the four cron services or the web service are missing a documented trigger — but see §9 for scripts that have **no trigger of any kind**, scheduled or otherwise (they're manual-only, not misconfigured cron jobs).

---

## 4. Outbound Communications

### The two independent status fields — and the confirmed double-send bug

The **Email Queue** table has two separate status fields that different modules key off of:
- `Email Status` (`Pending`/`Sent`) — read by `module_18_vendor_initial_request_sender.py`
- `Reminder Status` (`Queued`/`Sent`/`Failed`) — read by `module_10_vendor_email_sender.py`

Two creators — `module_17_queue_initial_requests.py:190,192` (Initial Request) and `processor.py:383-386` (Deficiency Request) — stamp **both** fields (`Email Status="Pending"` and `Reminder Status="Queued"`) on the same new row. Given `run_pipeline.py`'s fixed execution order (module_17 → module_18 → module_19 → module_15 → module_10, `run_pipeline.py:34-38`), any Initial Request or Deficiency Request row gets sent once by `module_18` (which flips `Email Status→Sent` but never touches `Reminder Status`), and then **is sent again later in the same run by `module_10`**, because `module_10`'s query is `Reminder Status='Queued' AND Send After<=NOW()` (`module_10_vendor_email_sender.py:64`) with no awareness that `module_18` already delivered it. **This is a confirmed, not hypothetical, duplicate-send bug that fires on essentially every pipeline run that creates an Initial Request or Deficiency Request email.** See §9 for the recommended fix direction.

### The ladder — `module_22_followup_ladder.py`

Two ladder types, both a Day 0 → 2 → 4 → 6 state machine, tracked in the **Outbound Follow-Ups** table:
- `LADDER_TYPE_SHORT_DATED` — triggered from `module_20_short_dated_check.py`, itself called synchronously inside `module_7b_requirement_validator.py:878-879` for every certificate with a policy expiring in ≤30 days (14-day cooldown per certificate via `Short-Dated Flagged At`).
- `LADDER_TYPE_NONCOMPLIANCE` — triggered from `module_7b_requirement_validator.py:835` when a vendor's decision enters `{Has Open Items, Needs Review, Missing Coverage}`.

**Timing — confirmed business-days, not calendar-days.** `create_ladder` (`module_22_followup_ladder.py:299-440`) anchors `Day 0 Send At` via `send_now_or_defer()` (immediate if within business hours, else snapped to next business-day 8am), then `Day 2/4/6` are `add_business_days(day_0, 2/4/6)` — **2, 4, and 6 *business* days out**, not 2/4/6 calendar days. Business hours (`module_21_business_hours.py`) are confirmed **Mon–Fri, 8:00am–6:00pm America/New_York**, no holiday handling.

- **Day 0 → Day 2 → Day 4:** each stage only advances once `module_10` calls back `record_send_success` after an actual send (`module_22_followup_ladder.py:937-971`, wired via `module_10_vendor_email_sender.py:117-133`). A queued-but-unsent email blocks the next stage indefinitely — there's no timeout. **The stamping call is itself wrapped in try/except that only logs on failure (`module_10_vendor_email_sender.py:129-133`)** — if the Airtable write to the ladder record fails, the Email Queue row is still marked `Sent`, but the ladder can get permanently stuck at its current stage with no retry and no alert.
- **Day 6 is internal-only, not vendor-facing.** It sets `Stage="Day 6 Escalated"` (`Status` stays `Active`, waiting on a human) and emails **Haley** (`config.OWNER_EMAIL`) a pre-drafted GC-facing email for her to manually copy and send from her own mailbox (`module_22_followup_ladder.py:563-655`). The code's own `TODO` (`:566-569`) calls this an interim flow pending a "Pending Escalations" dashboard that's explicitly out of scope for this build.
- **Halting:** an inbound reply matching the ladder's named insured (fuzzy sender/subject/body match, including vendor aliases) halts the ladder (`check_response_match`/`terminate_ladder`, called from `email_monitor.py`); a new compliant certificate also halts all active ladders for that vendor.
- **Bounces:** first bounce on the original sender retries to the PDF's producer/agent contact; a second bounce (or a bounce with no contact left) escalates to a terminal `Bounced-Escalated` stage — again a manual/internal flow, not an automatic resend.
- **Duplicate-ladder guard:** `create_ladder` skips creation if an `Active` ladder of the same type already exists for the same vendor/client — added after a documented prior incident of **1,232 duplicate ladders** accumulating from ~8 stuck assignments before the guard existed (`module_22_followup_ladder.py:369-370`).

### Confirmed orphaned queue — module_8b alert emails are never sent

`module_8b.queue_email` (`module_8b.py:276-299`) sets `Email Status="Pending"` but **never sets `Reminder Status`** — and its email types (`Cancellation Alert`, `Reinstatement Request`, `Reinstatement Alert`, `Endorsement Alert`) aren't `Initial Request`/`Deficiency Request`, so `module_18` never matches them either. `module_10`'s own type-routing table explicitly recognizes all four types (`module_10_vendor_email_sender.py:49-52`), strongly suggesting `module_10` was meant to send them — but since `Reminder Status` is never stamped `Queued`, `module_10`'s query never matches these rows. **Every cancellation/reinstatement/endorsement alert email queued by `module_8b` is permanently stuck and never sent by anything in the current wiring.** Worth spot-checking live Airtable for stale `Email Status="Pending"` rows of these four types with a blank `Reminder Status` to confirm the scale of this in production.

### Failure handling

Neither `module_10` nor `module_18` retries a failed send automatically. `module_10` sets `Reminder Status="Failed"` on a non-202 SendGrid response or any exception (`module_10_vendor_email_sender.py:110-114`) — that status is terminal; nothing re-queries `Reminder Status='Failed'`, so a human has to manually reset the field. `module_18` simply leaves `Email Status="Pending"` on failure, which **will** be retried on the next run (different failure semantics from `module_10` — worth knowing which sender a given failed row went through).

### Other outbound flows

- **`module_19_requirements_followup.py`** is unrelated to the vendor ladder — it nags **Haley**, not vendors, about clients who haven't returned their insurance requirements yet: a Day-3 reminder, a Day-7 escalation, and an annual review nudge, all keyed off `Clients.Requirements Status`.
- **`module_15_email_queue_builder.py`** Part 1 (live) builds policy-expiration reminder emails, using `module_12_vendor_reminder_engine.get_vendors_needing_reminders()` and writing `Insurance Policies.Last Reminder Threshold` after a successful queue so the same threshold doesn't re-fire. Part 2 (compliance-based deficiency reminders) is **entirely commented out** (`module_15_email_queue_builder.py:496-565`) with an explicit note that it was superseded by the ladder — meaning the `CLAUDE.md`-documented "module_7b writes Compliance Log, module_15 reads it back to personalize deficiency emails" flow is **not actually exercised** by `module_15`'s live code path; the real personalization for deficiency emails happens via a direct call from `module_7b_requirement_validator.py:836` to `module_15.build_deficiency_email_body`, bypassing `module_15.run()` entirely.

---

## 5. Payments & Client Lifecycle

All logic lives in `module_16_inbound_webhook.py`, the one live Flask service.

**`checkout.session.completed`** (`module_16_inbound_webhook.py:215-284`) — the only event that creates a Client. Resolves the price/tier via Stripe price ID first, falling back to matching the checkout amount in cents (`AMOUNT_CENTS_TO_TIER = {14900: "Starter", 39900: "Growth", 79900: "Scale"}`, `:103`); detects a trial by whether a coupon/promotion code was applied (retrieved via a separate `Session.retrieve` with `expand`, since the webhook payload itself doesn't include discount details). Creates a **Clients** record (guarded against re-creation by an existing-email check, `:344-347`) with `Subscription Status = "Trial"` (30-day `Trial Ends`) or `"Active"`, `Tier`/`Subscription Tier` (only written if a known tier resolved — an unresolved tier leaves those fields blank rather than guessing), Stripe Customer/Subscription IDs, and a default `Requirements Status = "Pending — Awaiting Reply"`. Sends an owner-notification email to Haley and a consolidated welcome email to the customer (deduped via a `Welcome Sent` checkbox lookup, `:421-429`).

**`customer.subscription.deleted`** — marks `Subscription Status = "Canceled"`, records `Subscription Ended`, sends a cancellation-confirmation email (states data is retained 90 days before archival — that 90-day retention/archival mechanism is **not implemented anywhere in this codebase**; `Clients.Archive Date` is a formula field per the live schema, so the actual archival trigger is UNVERIFIED and likely lives in an Airtable automation not visible here).

**`invoice.payment_failed`** — marks `Subscription Status = "Past Due"`. No email (comment confirms Stripe's own dunning emails are relied on instead).

**`invoice.paid`** — flips `Past Due → Active` only if the client was currently `Past Due`; no-op otherwise.

**`customer.subscription.updated`** — three narrow reactions: (1) `cancel_at_period_end` flipping to `True` is logged only, no Airtable write; (2) a price-ID change to a different resolved tier updates `Tier`/`Subscription Tier`; (3) a coupon disappearing from a `Trial`-status client flips them to `Active` (post-trial conversion).

**Every handler is wrapped so the webhook always returns HTTP 200** (`module_16_inbound_webhook.py:190-212`), specifically to prevent Stripe retries from causing duplicate side effects (double emails, etc.) on a partial failure — failures are logged, not surfaced anywhere else.

**`/api/create-checkout-session`** (`module_16_inbound_webhook.py:148-172`) — a `GET` endpoint taking a `?tier=starter|growth|scale` param, resolving the Stripe Price ID and Coupon ID from environment variables at request time (not import time, so a Railway env change takes effect without a redeploy), and creating a subscription-mode Checkout Session with `discounts=[{"coupon": coupon_id}]` — note this **always applies whatever coupon is configured for that tier's env var**, unconditionally, to every checkout through this endpoint; there's no logic gating whether a discount should apply. The most recent commit in this repo (`b13f774`) fixed a prior conflict between `allow_promotion_codes` and `discounts` on this same call — confirming this endpoint has been actively iterated on recently.

**Pricing tiers — code-confirmed, live-Stripe-unconfirmed.** The three-tier model (**Starter / Growth / Scale**, vendor caps 30/100/300) is consistent across `module_16_inbound_webhook.py:96-103`, `bulk_vendor_import.py:27` (`TIER_CAPS`, with an explicit comment "Pro is intentionally not present — V1 ships 3 tiers only"), and the live Airtable `Subscription Tier` field description on the Clients table ("Starter=30, Growth=100, Scale=300"). **`SETUP_NOTES.md` names three Stripe Payment Links "Crew, Foreman, General"** — a direct naming mismatch against the code's Starter/Growth/Scale. **UNVERIFIED** whether Crew/Foreman/General are just customer-facing marketing names mapped internally to the same three price IDs, or a stale/abandoned naming scheme — this can only be resolved by checking the live Stripe dashboard, which is not accessible from this environment. **I could not verify current Stripe pricing amounts, active coupons, or promo logic against the live Stripe dashboard at all** — everything in this section is what's hardcoded/referenced in the repo, not confirmed against Stripe itself.

---

## 6. Client Portal (Softr)

**There is no API layer in this repository serving the Softr portal.** All four references to Softr in the codebase are comments/notes, not integration code:

- `module_16_inbound_webhook.py:413` — after a new Stripe signup, the owner-notification email literally instructs Haley: *"Next step: Add them manually in Softr at app.carolinacompliancesolutions.com."* **New-client provisioning into the portal is a manual step, not automated.**
- `exception_manager.py:299` — a deficiency-alert email is deliberately left disabled with the comment *"GC views deficiencies in Softr portal, email noise at this stage."*
- `legal_disclaimer.py:66` — the `SUMMARY_DISCLAIMER` string is marked *"Customer-visible in the Softr portal."*
- `bulk_vendor_import.py:53` — warns that a Client record missing `Primary Contact Email` will break "Softr portal filtering."

Taken together, this points to Softr reading **Airtable directly** via its native no-code Airtable connector, filtering/scoping views by fields this repo writes (e.g. `Primary Contact Email`), with no REST call into any Python code here. **UNVERIFIED beyond that inference** — there's no Softr API key, webhook, or SDK reference anywhere in the source, so the exact data-isolation mechanism (per-client filtered views, row-level security, etc.) is configured entirely inside Softr itself and isn't visible from this repo. **You should confirm directly in the Softr builder that each client's view is actually scoped to their own linked records** — this audit cannot verify that from the code.

**Legal-language check (this was a specific ask) — status labels look correctly hedged.** The compliance status vocabulary used everywhere in code and (per the disclaimer comment above) surfaced to the portal is `"Matches Requirements"` / `"Has Open Items"` / `"Needs Review"` / `"Missing Coverage"` / `"Expired"` (`module_7b_requirement_validator.py:27-30`) — none of this uses "Verified," "Compliant" as a bare claim, or similar language that would imply CCS is attesting to actual insurance coverage. Every outbound email and the portal-visible `Compliance Decision Summary` field appends an explicit disclaimer: *"We do not verify coverage, determine coverage adequacy, or make insurance recommendations... not a determination about your actual insurance coverage"* (`legal_disclaimer.py:34-45`, `legal_disclaimer.py:69-72` for the shorter portal-facing version). This is a deliberate, well-built guardrail, current as of a documented "pre-launch must-fix sweep" review dated 2026-04-26 (`legal_disclaimer.py:26`). **I cannot verify what Softr actually renders** (e.g., whether a portal-side label, icon, or column header re-introduces "verified" language independent of these Airtable field values) — that would require inspecting the live Softr interface directly, which this audit did not have access to.

---

## 7. Data Model

Pulled directly from the live Airtable base via the Airtable API (not from code assumptions). Base: **Carolina Compliance Solutions OS** (`appCGgww0Pt7KE04u`).

| Table | Purpose | Key fields | Relates to |
|---|---|---|---|
| **Clients** (`tbltnBIWke20IEI3K`) | General-contractor customers; also holds the full Stripe subscription lifecycle | Client Name, Client Status, Primary Contact Email, Subscription Status/Tier/Started/Ended, Trial Ends, Stripe Customer/Subscription ID, Requirements Status, Certificate Holder, COI Contact Email/Name/Role (Day-6 escalation routing) | → Client Requirements, Vendor Client Assignments, Vendors, Insurance Certificates, Email Queue, Outbound Follow-Ups, Vendor Requirement Overrides, Compliance Log |
| **Vendors** (`tblsOphSd5DKSZEro`) | Subcontractors being tracked | Vendor Name, Vendor Email, Compliance Status, GL/WC/Auto Status + Expiration Date (written by module_7b), Additional Insured/Waiver On File, Next Expiration Date, Send Request (checkbox that triggers module_17), Initial Request Sent (30-day dedup), Aliases, DBA Names, Agency Email, Broker Email | → Client Link, Insurance Policies, Insurance Certificates, Vendor Client Assignments, Outbound Follow-Ups |
| **Vendor Client Assignments** (`tblpYKywfs0YHiQ98`) | The active many-to-many link between a vendor and a client — **this is the table module_7b actually evaluates against** | Vendor Link, Client Link, Active, Compliance Status, Expiration Status, Last Evaluated | Vendor ↔ Client |
| **Client Vendors** (`tblYPs2h9jxT3OL9H`) | A second, largely-separate vendor/client junction table — used only by `bulk_vendor_import.py` and the disabled `module_11_task_generator.py` (confirmed by grep: neither `processor.py` nor `module_7b_requirement_validator.py` reference it) | Notes, Assignee, Status, Attachments, Tracking Status | Client, Vendors, Insurance Policies, Insurance Certificates — **overlapping purpose with Vendor Client Assignments; see §9** |
| **Insurance Policies** (`tblpPcmm5ANE0bMNB`) | Individual policy lines (GL, WC, Auto, etc.) extracted from certificates | Policy Type, Policy Number, Expiration/Effective Date, Expiration Status, Last Reminder Threshold, Coverage Limits, Additional Insured/WOS On File, Timing Flags, Claims Made Flags, Policy Basis, Retroactive Date | Vendor Link, Client Vendors, Insurance Certificates |
| **Insurance Certificates** (`tbl0IH6zQQsXBff3l`) | One row per received COI document — **the table §2 traces documents into** | Named Insured, Review Status, Compliance Status, Compliance Evaluated At, Compliance Decision Summary, Compliance Failure Reasons, Deficiency Email Queue Status/Sent, Has Short-Dated Policies, Short-Dated Flagged At, Mid-Term Changes | Vendor Link, Client, Insurance Policies, COI Requests, Outbound Follow-Ups, Extraction Training Log |
| **Insurance Certificates (New)** (`tblemOEhQk9s68gTN`) | **Confirmed orphaned/legacy** — no pipeline code writes to this table; the field of the same name on the *Clients* table is a leftover link, and `dashboard_metrics.py` reads a field with this name that doesn't even exist on the table it queries (see §9 for the exact bug) | Certificate Name, Vendor, Client, COI File, Status | — |
| **Incoming Extractions** (`tblT88Ty6d6M766oY`) | Raw Claude-extraction records, one per intake document — the working table for Stages 3-4 of §2 | Raw JSON (canonical extraction record), Processing Status, Match Status/Method/Confidence, Matched Vendor/Client, Cancellation fields, Confidence Score, Manually Reviewed By, Reprocess Triggered At | Matched Vendor, Matched Client, Matched Client Request, Insurance Policies |
| **Incoming Documents** (`tblHGDYIdA4SzAjZG`) | One row per inbound email (before/regardless of extraction) | Sender Email, Subject, Status, Event Type, Classification Confidence/Method, PDF R2 Key, Source Email CC/Message ID/Body Snippet, Skip Reason | — |
| **Client Requirements** (`tblFGQ6XgOIHSWtQN`) | Per-client, per-policy-type minimum requirements | Policy Type, Required, Minimum Limit, Additional Insured/Waiver/Primary Noncontributory Required, Requirements Date Set, Source Notes (audit trail) | Client Link |
| **Vendor Requirement Overrides** (`tblKVIdTYwF1Sp5Iw`) | Manual, GC-approved exceptions to a specific requirement for a specific vendor | Override Required/Minimum Limit/Additional Insured/Waiver, Override Reason, Approved By, Approval Date, Expiration Date, Active, Exception Status | Vendor Link, Client Link |
| **Email Queue** (`tblCeRCf6RToTFkbL`) | Outbound email staging — see §4 for the two-status-field bug | Primary Email, CC Emails, Subject, Body, Email Type, Email Status, Reminder Status, Record Status, Send After, Sent At | Vendor, Client, Policy, Outbound Follow-Ups |
| **Outbound Follow-Ups** (`tbl3T8AjTrzrg2PY4`) | The follow-up-ladder state machine described in §4 | Ladder ID, Ladder Type, Stage, Status, Day 0/2/4/6 timestamps, Bounce Count, Termination Reason | Vendor Link, Client Link, Certificate Link, linked Email Queue rows |
| **Compliance Log** (`tblxdt7DT6V3JCQcW`) | "Immutable audit trail of every compliance decision" (table description) — written by module_7b on every run | Timestamp, Vendor/Client Name, Decision, Failure Reasons, Previous Decision, Decision Changed | Vendor Link, Client Link — **grows unboundedly, see §9** |
| **COI Requests** (`tbl5QoSfwZWRxzSdY`) | Tracks a request thread from a specific inbound email through to resolution | Status, Requested Policy Numbers, Request Source Email, Last COI Sender Email | Vendor, Client, Incoming Extractions, Insurance Certificates |
| **Tasks** (`tbl4m6HwVuq29HM2V`) | Manual task tracking — only written by the **disabled** modules 6/11/`task_generator.py` | Task Name, Due Date, Status, Priority | Client Vendor, Vendor Link, Policy Link |
| **Templates** (`tblvKejr5NyKZYT0f`) | Reusable email templates | Name, Subject, Body, Type | — UNVERIFIED whether any live email-builder module actually reads this table; none of the files audited referenced it by table ID |
| **Extraction Training Log** (`tblBW6HkbSVYkN2PN`) | Manual correction log from Haley's review, for improving extraction accuracy over time | Field Name, Pipeline Value, Correct Value, Reviewer | Certificate Link |
| **Prospects** (`tblk5PynfuHm92bsT`) | Sales/outreach CRM tracking — confirmed **entirely unrelated to the compliance pipeline** (zero code references) | Company Name, Contact, Status, Reply Category, Gmail Thread ID | — |
| **Inactive Account Replies** (`tbldw0kEQnlhNEPw6`) | Tracks replies from clients with inactive/canceled accounts | Sender Email, Last Reply Sent/Type, Follow-Up Needed | Client |

**Confirmed table-naming confusion worth cleaning up:** the base has two pairs of near-duplicate tables — **Insurance Certificates** vs. **Insurance Certificates (New)**, and **Vendor Client Assignments** vs. **Client Vendors**. In both pairs, only the first name is what the live pipeline actually uses; the second is legacy/partial and only touched by dead or manual-only code. A future editor (or Haley herself, six months from now) could easily write new logic against the wrong one — see the concrete bug this already caused in §9.

---

## 8. Environment Variables & Secrets

Names only, grouped by consuming service — no values reproduced.

**Shared / `config.py` (used by every pipeline script and the web service):**
`AIRTABLE_API_KEY`, `AIRTABLE_BASE_ID`, `AIRTABLE_TABLE_NAME` (Incoming Documents table name override), `ANTHROPIC_API_KEY`, `HALEY_EMAIL` (owner notification recipient), `UPLOAD_DIR`, `COI_REVIEW_CONFIDENCE_THRESHOLD`.

**`email_monitor.py` / IMAP intake:**
`IMAP_HOST`, `IMAP_PORT`, `EMAIL_ADDRESS` (must exactly equal `coi-intake@carolinacompliancesolutions.com`, enforced at startup), `EMAIL_PASSWORD` (Gmail App Password, must be 16 characters after stripping spaces, enforced at startup).

**Outbound email (SendGrid), used across module_10/15/17/18/19/8b/16:**
`SENDGRID_API_KEY`, `SENDGRID_FROM_EMAIL`, `INBOUND_EMAIL` (the vendor-facing `coi@` reply-to address).

**Cloudflare R2 (`r2_storage.py`, used by `email_monitor.py` and `airtable_backup.py`):**
`R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`.

**Stripe / billing (`module_16_inbound_webhook.py` only):**
`STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_PRICE_STARTER`/`STRIPE_PRICE_GROWTH`/`STRIPE_PRICE_SCALE`, `STRIPE_COUPON_STARTER`/`STRIPE_COUPON_GROWTH`/`STRIPE_COUPON_SCALE`, `CHECKOUT_SUCCESS_URL`, `CHECKOUT_CANCEL_URL`.

**Web service runtime:**
`PORT` (Gunicorn bind port), `WERKZEUG_RUN_MAIN` (Flask internal, not something you set).

**Unused/legacy — present in code history but not required by the live pipeline:**
`OPENAI_API_KEY` — only referenced by the stale `README.md`/`AI_SYSTEM_CONTEXT.md` documentation describing an old OpenAI-based extractor; no live `.py` file reads this variable via `os.getenv`/`os.environ`.

**`.env.example` is stale** — it documents only 8 of the ~24 variables actually in use (IMAP, Airtable, upload dir only; missing Anthropic, R2, Stripe, and SendGrid entirely). If you're ever rebuilding `.env` from scratch, use the list above, not `.env.example`.

---

## 9. Known Issues Found During This Audit

Ranked roughly by operational impact. Every item below is confirmed from the code as it exists today, not speculation.

1. **Confirmed duplicate-send bug — every Initial Request and Deficiency Request email is sent twice.** `module_17_queue_initial_requests.py:190,192` and `processor.py:383-386` both stamp `Email Status="Pending"` *and* `Reminder Status="Queued"` on creation. `module_18_vendor_initial_request_sender.py:98` sends on `Email Status='Pending'` and only updates `Email Status`/`Sent At` on success — it never touches `Reminder Status`. `module_10_vendor_email_sender.py:64` independently sends on `Reminder Status='Queued'` with no knowledge that module_18 already delivered it. In the fixed pipeline order, this fires on effectively every run that creates one of these two email types. **This is the single highest-priority fix in this audit** — vendors are plausibly receiving duplicate COI request emails today.

2. **Confirmed — `module_8b.py`'s cancellation/reinstatement/endorsement alert emails are never sent.** `module_8b.queue_email` (`module_8b.py:276-299`) sets `Email Status="Pending"` but never sets `Reminder Status`, and its email types don't match `module_18`'s type filter either. `module_10` recognizes the types in its routing table but can never see them because `Reminder Status` is never `"Queued"`. Worth checking live Airtable for a backlog of stuck rows.

3. **`CLAUDE.md`'s documented webhook-based inbound-email path does not exist in the code.** The SendGrid Inbound Parse route and `COI_TARGET_FILE` business-hours-immediate-processing flow described in `CLAUDE.md` were removed (confirmed by `module_16_inbound_webhook.py:19-21`'s own docstring and a repo-wide grep for both). **All COI intake is schedule-driven on whatever cadence `cron-pipeline` actually runs at** (documented as once daily) — there is no same-day-of-arrival processing guarantee. This is the most consequential documentation-vs-code gap in the repo and should be reconciled or the webhook capability rebuilt, since "same-day request turnaround" is presumably part of the value proposition.

4. **Gating in `email_monitor.py` doesn't actually gate anything downstream.** Oversize/unsupported-type checks only update Airtable status; the file stays in `uploads/` and `extractor.py` has no size check of its own, so an oversized PDF still gets sent to Claude for extraction on the same run. Only `.tiff`/`.tif` files are genuinely excluded (by extension mismatch, not by design). See §2 for full detail.

5. **`dashboard_metrics.py:105` reads a field that doesn't exist on the table it queries.** `_print_compliance_overview` loops over **Vendors** records and reads `fields.get("Insurance Certificates (New)")` — but per the live Airtable schema, the Vendors table has a field called `"Insurance Certificates"` (`fldT3dzS5DW6Rsdef`), not `"Insurance Certificates (New)"` (that field exists only on the **Clients** table, linking to the orphaned `Insurance Certificates (New)` table). This lookup will return `None` for every single vendor record, so the "MISSING CERTIFICATES" count this script prints is **always equal to the total vendor count**, regardless of actual data. This script isn't wired into any cron, so it only misleads if run manually — but it will always be wrong if it is.

6. **Two separate, functionally-overlapping vendor-import scripts with different safety guarantees.** `bulk_vendor_import.py` enforces the per-tier vendor cap (`TIER_CAPS`); `import_vendor_roster.py` does not. Both are manual-only (neither is wired into any cron), so this is an operator-training issue, not an automatic risk — but using the wrong one could silently blow past a client's subscription cap. Additionally, `import_vendor_roster.py`'s own duplicate-detection formula (`import_vendor_roster.py:114`) appears to rely on `FIND(record_id, ARRAYJOIN(...))` against a linked-record field — `ARRAYJOIN` on a link field returns the linked record's *display text*, not its record ID, which is the exact bug `bulk_vendor_import.py`'s own code comments (lines 56-61) warn about elsewhere in this same repo. This means `import_vendor_roster.py`'s "already imported" dedup check likely silently never matches, risking duplicate vendor creation on repeated use.

7. **Compliance Log grows unboundedly with no dedup.** `module_7b_requirement_validator.write_compliance_log` appends a new row for every vendor×client pair on **every pipeline run**, whether or not the decision changed (`Decision Changed` is recorded as metadata, not used as a skip condition). This is defensible as an audit trail, but worth a deliberate decision about retention/archival rather than letting it grow indefinitely — especially since `processor.py` may *also* be triggering a compliance evaluation inline right after certificate creation (§2, Stage 4) — UNVERIFIED whether that inline call reaches `write_compliance_log` too, which would mean some certificates get logged twice per day.

8. **Exception-expiry alert emails are built but disabled.** `exception_manager.py:227-241` — the code path that would email someone when a Vendor Requirement Override is about to expire is commented out with the note "post-launch feature, too complex for early clients." The `Exception Status` field still updates to `"Expiring Soon"`/`"Expired"` correctly, but no one is notified. Two related functions, `queue_deficiency_alert()` and `record_human_override()`, are fully built but have zero call sites anywhere in the repo — human-approved exceptions are never actually logged to an audit trail despite the function existing to do exactly that.

9. **`compliance_checker.py` is entirely dead code** — zero references anywhere else in the repo — but duplicates, with simpler/different logic, what `module_7b_requirement_validator.py` actually does live. A future reader could easily mistake it for active logic. Recommend explicit archival or deletion.

10. **Several evaluator code paths are "wired in" but structurally unreachable** because the only production call site never passes the parameter that gates them: `endorsement_evaluator.py`'s Level-3 endorsement-document evidence (`has_endorsement_doc` never passed from `module_7b_requirement_validator.py:631`) and its endorsement-date-mismatch check (`endorsement_effective_date`/`policy_effective_date` never passed); `claims_made_evaluator.py`'s tail-coverage-on-cancellation check (`is_cancellation` never passed from `module_7b_requirement_validator.py:658`, so it's always `False`); `midterm_change_detector.py`'s cancellation/expiry-conflict check (`cancellation_effective_date`/`certificate_expiry_date` never passed from `processor.py:2163`). None of these crash — they just silently never fire.

11. **`module_8b.handle_reinstatement` re-validates using a weaker legacy code path.** It calls the old `module_7b_requirement_validator.validate_vendor()` (a simple pass/fail check with no structured reasons) instead of the modern `evaluate_assignment()` used everywhere else in the pipeline — meaning a reinstatement can be marked compliant without the endorsement-evidence-level, claims-made, active-exception, or grace-period logic a normal pipeline pass would apply.

12. **`README.md` and `AI_SYSTEM_CONTEXT.md` are both substantially stale** and should not be trusted for current architecture. Both describe an OpenAI GPT-4o-based extractor (current code uses Anthropic Claude, confirmed via `extractor.py` and `CLAUDE.md`), a 6-module pipeline ending at `task_generator.py` (the real pipeline has ~12 stages plus daily cron tasks and modules 7B/8/8B/10/15/17/18/19), and `python main.py` as the way to run the email monitor (the real pipeline runs `email_monitor.py` directly — `main.py` is an orphaned duplicate entry point). `.env.example` is also stale (documents 8 of ~24 real variables).

13. **`SETUP_NOTES.md`'s Stripe Payment Link names ("Crew, Foreman, General") don't match the code's tier names ("Starter, Growth, Scale").** UNVERIFIED which is authoritative without checking the live Stripe dashboard — flagged in §5.

14. **`task_progress.md` at the repo root is not about Carolina Compliance Solutions at all.** Its content (admin pages, notification health APIs, org-scoping) matches an unrelated codebase present in this same working directory under `.claude/worktrees/sad-goldberg/` and `.claude/worktrees/wonderful-mayer/` — two separate git worktrees containing a different product's source (`admin_ui.py`, `notification_health_api.py`, etc.). These worktree directories are leftover from other work sessions and are not part of the deployed CCS product — worth cleaning up so a future reader doesn't confuse them with this repo's real code.

15. **Three separate, mutually-redundant "create a task when a policy expires" implementations exist** (`module_6_task_creator.py`, `module_11_task_generator.py`, `task_generator.py`), all disabled/orphaned. `module_11_task_generator.py:103` has a live `KeyError` bug (references a `"Vendor Link"` dict key that `load_existing_open_task_keys` never populates) that would crash immediately if this code were ever turned back on.

16. **`vendor_upload_portal.py` is non-functional as written** — `from auth import authenticate_user` (`vendor_upload_portal.py:9`) imports a module that doesn't exist anywhere in the repo. It would raise `ImportError` on any attempt to run it. It also has no Airtable integration and no real multi-tenant isolation logic even in principle.

17. **`auth_middleware.py` is dead code with fake hardcoded authentication** (`token == "valid-token"`, `token == "admin-token"`, every token maps to the same hardcoded `"org-123"`). It's not imported by the live Flask app today, so it poses no active risk — but it's a landmine if anyone later wires it into a route assuming it's real auth.

18. **`Insurance Certificates (New)` and `Client Vendors` tables are legacy/confusing duplicates** of the tables the live pipeline actually uses (`Insurance Certificates`, `Vendor Client Assignments`) — see §7. Recommend renaming or archiving to prevent future accidental use.

19. **`notifications.db`** (SQLite, mentioned in `CLAUDE.md` as used by "the webhook service for local dedup") — this audit found **no code path in `module_16_inbound_webhook.py`** (the only live webhook service) that reads or writes this file. **UNVERIFIED**: either this is genuinely unused today (a leftover from the removed SendGrid Inbound Parse webhook, which would have needed local dedup), or some other component references it that wasn't in scope for this audit. Worth a direct grep/check before assuming it's load-bearing.

---

## 10. How to Verify the Pipeline Is Working

A manual end-to-end health check. Budget about 15 minutes of active time, spread across the next 24 hours (because intake is currently once-daily — see §2/§9 finding #3).

**Step 1 — Send a test COI.**
Email a real or sample ACORD 25 PDF to `coi@carolinacompliancesolutions.com` from an address you control, with a subject like `TEST COI - <today's date>`. Use a Named Insured that either matches an existing test Vendor, or one you're prepared to see land as `Unmatched`.

**Step 2 — Wait for the next `cron-pipeline` run.**
Check the Railway dashboard for the `cron-pipeline` service's schedule and next-run time (don't trust `railway.toml`'s comment — confirm live). Wait until that run completes.

**Step 3 — Check Incoming Documents.**
In Airtable, find the new row (search by Subject or Sender Email). Confirm `Status` is not `Failed`, and that `Event Type`/`Classification Confidence` look sane (event type should be something like "new COI", not "bounce" or "auto-reply").

**Step 4 — Check Incoming Extractions.**
Find the row with matching `Source Filename`. Confirm `Processing Status` — expect `"Processed"` if the vendor/client matched cleanly, `"Pending Review"` with `Review Reason = "Low Confidence"` if extraction confidence was under 0.95, or `"Unmatched"` if no vendor matched (remember: `"Unmatched"` is a separate status from `"Needs Review"` per §2/§9). Spot-check `Raw JSON` against the actual PDF to sanity-check the extraction.

**Step 5 — Check Insurance Policies and Insurance Certificates.**
If Step 4 showed `"Processed"`, confirm a new **Insurance Certificates** row exists linked to the right Vendor and Client, with a populated `Compliance Status`, and that the expected policy lines appear in **Insurance Policies** with correct `Policy Number`/`Expiration Date`/`Expiration Status`.

**Step 6 — Check Compliance Log.**
Confirm a new row was appended for this vendor×client pair with a `Decision` matching what you saw on the certificate, and (if the vendor was already known to be non-compliant on some other line) sane `Failure Reasons` text.

**Step 7 — If the test vendor was intentionally non-compliant, check Email Queue and Outbound Follow-Ups.**
Confirm a Deficiency Request or ladder Day-0 email row was created with `Email Status`/`Reminder Status` both present. **Given the confirmed double-send bug (§9, item 1), check whether you receive the email twice** at the test address — that's expected today, not a new bug, but worth confirming it's still happening so you know whether it's been fixed yet.

**Step 8 — Confirm the send actually happened.**
On the next `cron-pipeline` run (or whenever `module_10`/`module_18` next execute), confirm the Email Queue row(s) flip to `Sent`, and that you actually received the email(s) at the test address.

**Step 9 (weekly) — Confirm the backup ran.**
Check the R2 bucket (via the Cloudflare dashboard, or `verify_r2_upload.py` if you have shell access to the deployment) for a fresh weekly CSV export dated after the last Sunday 6am UTC.

**Step 10 (as needed) — Confirm the follow-up ladder advances.**
If you have a certificate sitting in a non-compliant state, watch its linked **Outbound Follow-Ups** row over several business days: `Stage` should progress `Day 0 Sent → Day 2 Sent → Day 4 Sent → Day 6 Escalated` on schedule (2/4/6 *business* days apart), and you should receive a Day-6 internal escalation email if it gets that far without a vendor response.

If any step doesn't match what's described here, the corresponding stage in §2–§4 tells you exactly which script and Airtable field to go look at.
