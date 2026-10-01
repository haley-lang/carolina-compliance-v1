# Carolina Compliance Solutions — Business Operations Reference

**Audience:** Haley, day-to-day. Not a developer doc — [SOP.md](SOP.md) is the technical companion and is cross-referenced throughout instead of repeated.

**Grounding:** Every claim below is read directly from the code in this repo as of 2026-08-08 (including uncommitted working-tree changes — `git status` shows `run_pipeline.py`, `railway.toml`, `module_10_vendor_email_sender.py`, and `module_18_vendor_initial_request_sender.py` modified from the last commit). Where something lives only in Airtable's UI, Softr, or the Stripe dashboard, it's marked **UNVERIFIED** — I can't see those from the code and didn't guess.

**Important — this document sometimes disagrees with SOP.md.** SOP.md was audited against a slightly earlier state of the code. Two things changed since:
1. **The duplicate-send email bug SOP.md §9 item 1 describes is fixed.** `module_18_vendor_initial_request_sender.py` now sends *only* "Initial Request" emails and reads/writes `Reminder Status` (not `Email Status`); `module_10_vendor_email_sender.py:72-77` explicitly excludes "Initial Request" from its own query. The two senders can no longer claim the same row — see the code comments at `module_18_vendor_initial_request_sender.py:88-104` and `module_10_vendor_email_sender.py:65-71` for the fix itself.
2. **The pipeline cadence recommendation changed from once-daily to every 15 minutes.** `railway.toml`'s comment block now reads `cron-pipeline: python run_pipeline.py (*/15 * * * *)` with a note that this is deliberate, not accidental: *"run_pipeline.py runs email_monitor.py (IMAP intake) as its own first step, so it needs to fire often to keep COI turnaround from lagging a full day behind arrival"* (`railway.toml:16-20`). A new file-lock guard (`PIPELINE_LOCK_PATH`, `run_pipeline.py:26-52`) makes it safe to run that often — an overlapping run just exits immediately instead of double-processing.

**Same caveat SOP.md flags applies here too:** `railway.toml` is explicit that these schedules are *recommended*, not enforced — actual cron timing lives in the Railway UI, which this repo can't see. **Confirm the live schedule in the Railway dashboard before trusting any timing number below.**

---

## 1. What Happens When a COI Comes In (plain-English walkthrough)

Technical version: [SOP.md §2](SOP.md#2-inbound-pipeline-coi-intake). This is the same trail, in plain terms, with the timing corrected for the current (every-15-minutes) cadence.

1. **A vendor emails a COI to `coi@carolinacompliancesolutions.com`.** (That's the address vendors see everywhere — request emails, deficiency emails, etc. The actual mailbox the system reads via IMAP is `coi-intake@carolinacompliancesolutions.com`; how mail addressed to `coi@` lands there is a Google Workspace forwarding/alias setup outside this repo — **UNVERIFIED**.)

2. **Within about 15 minutes** (assuming the recommended cron cadence is what's actually live — confirm in Railway), the pipeline runs and picks up the new message: it's logged as a row in **Incoming Documents**, the attachment is saved, and the system classifies what kind of email it is (a new COI vs. a bounce vs. an auto-reply vs. something with no usable attachment).

3. **In that same pipeline run**, if it looks like a real COI, Claude reads the PDF and pulls out the policy details (carrier, policy numbers, coverage types, limits, dates, endorsements). This becomes a row in **Incoming Extractions**.

4. **Still the same run**, the system tries to match the document to a known vendor (by name, alias, or DBA) and to the right client (by certificate holder). If the match is confident, it's automatic. If it's a fuzzy or weak match, or extraction confidence was low, it's set aside for you to review instead of guessing (`Processing Status = "Low Confidence"`, `"Needs Review"`, or `"Unmatched"` — see SOP.md §2 Stage 4 for the exact confidence thresholds).

5. **If it matched cleanly**, the certificate and its policy lines are written to **Insurance Certificates** / **Insurance Policies**, and the system immediately checks that vendor's coverage against the client's requirements (**Client Requirements** table). This produces a compliance decision — `Matches Requirements`, `Needs Review`, or `Missing Coverage` — written to that vendor's assignment for that client.

6. **If something's missing or doesn't meet the requirement**, the system doesn't email the vendor on the spot from that first check (see §2 below for why — there's a real code path for an "immediate" deficiency email, but it's gated on a status value the evaluator never actually produces in normal use, so it doesn't fire). What actually happens is a few steps later in the same run: a fresh, full re-evaluation of every vendor/client pair (the standalone Module 7B pass) runs, and *that* pass is what starts the vendor follow-up sequence — see §2/§3 below.

**Bottom line on timing:** assuming the recommended every-15-minute cadence is what's actually configured in Railway, a vendor's COI is typically logged, extracted, matched, and evaluated for compliance within about 15–30 minutes of arrival, and a deficiency follow-up (if one is needed) is queued and sent in that same pipeline run. This is a significant change from the once-a-day cadence SOP.md's original audit found — confirm the live Railway schedule to be sure it's really this fast today.

---

## 2. The Deficiency / "Edits Needed" Flow

**What actually triggers a deficiency email to a vendor** is the standalone **Module 7B** pass (`module_7b_requirement_validator.py`, step 7 of `run_pipeline.py`'s sequence). For every active Vendor↔Client Assignment where the client's requirements are confirmed received, it runs `evaluate_assignment()` (`module_7b_requirement_validator.py:688-765`), which checks each required coverage line (General Liability, Workers Comp, Auto Liability, etc.) for:

- **Missing policy** — required type has no policy on file at all (`RC_POLICY_MISSING`)
- **Expired** — past the client's grace period (default 30 days, overridable per-requirement via a `Notes` field, `module_7b_requirement_validator.py:459-466`)
- **Coverage limit too low** — parsed per-occurrence/aggregate dollar amount below the client's minimum
- **Endorsement not evidenced** — Additional Insured, Waiver of Subrogation, or Primary & Noncontributory required but not shown on the certificate
- **Certificate holder mismatch** — the certificate doesn't list the client as certificate holder
- **Claims-made policy gaps** — retroactive date / tail coverage issues, when the policy is written on a claims-made basis

Each finding becomes a `REASON_CODE: human-readable message` line (e.g. `POLICY_MISSING: General Liability — required but no policy on file`). These are the exact lines that appear in the vendor's email — see the real template below.

**If the vendor has an active, GC-approved exception on file** (Vendor Requirement Overrides table) for that specific line, the deficiency is still recorded but annotated with the exception details rather than treated as a fresh gap (`module_7b_requirement_validator.py:668-681`).

**When a deficiency email actually goes out:** if the new decision is `Needs Review`, `Missing Coverage`, or `Has Open Items` (the last one only ever gets set by a cancellation notice — see §3), `trigger_post_evaluation_hooks()` starts a **Noncompliance Return** follow-up ladder (`module_7b_requirement_validator.py:832-863`, `module_22_followup_ladder.py`). The Day-0 email of that ladder *is* the deficiency email.

**The real wording** (`module_15_email_queue_builder.py:104-128`, `build_deficiency_email_body`):

> Hi {vendor name},
>
> Your submitted Certificate of Insurance on file with {client name} has been reviewed and documentation deficiencies have been identified.
>
> The following items require attention:
> - {reason 1}
> - {reason 2}
>
> Please send an updated Certificate of Insurance addressing these items to: coi@carolinacompliancesolutions.com
>
> Include {client name} — {vendor name} in the subject line.
>
> Questions? Reply to this email.
>
> Carolina Compliance Solutions
> coi@carolinacompliancesolutions.com

...followed by the standard legal disclaimer (`legal_disclaimer.py:34-45` — "we are not an insurance broker... do not verify coverage... not a determination about your actual insurance coverage").

**A worth-knowing wrinkle:** there's a *separate*, older "immediate" deficiency-email code path inside `processor.py` (`queue_deficiency_email_if_needed`, `processor.py:265-422`) that fires the moment a certificate is first processed, before the standalone Module 7B pass runs. It's gated on `compliance_result.outcome == "Has Open Items"` (`processor.py:2340`) — but the evaluator it calls (`evaluate_assignment`, same function as above) never actually returns that literal value; `"Has Open Items"` is only ever set directly by the cancellation handler (`module_8b.py:424,435`), a comment in the validator itself notes this explicitly (`module_7b_requirement_validator.py:343-348`). In practice, this means **the "immediate" deficiency email doesn't fire from a normal COI submission** — the ladder's Day-0 email (above) is the one that actually reaches vendors for ordinary missing-coverage/needs-review findings.

**If the vendor never responds — the escalation ladder — see §3 for the full timeline.**

---

## 3. The Full Follow-Up / Escalation Ladder

There are actually **three independent follow-up mechanisms**, not one. Knowing which one is running for a given vendor matters when you're troubleshooting.

### 3a. Noncompliance Return ladder (the main one — deficiencies)

Triggered by Module 7B when a vendor/client assignment evaluates to `Needs Review` or `Missing Coverage` (or `Has Open Items` from a cancellation — see 3c). State machine lives in **Outbound Follow-Ups**, driven by `module_22_followup_ladder.py`.

| Stage | Timing | What happens |
|---|---|---|
| **Day 0** | Immediate if sent during business hours (Mon–Fri, 8am–6pm ET); otherwise deferred to 8am the next business day (`module_21_business_hours.py:63-75`) | Deficiency email above, sent to the vendor's original sender address |
| **Day 2** | 2 *business* days after Day 0 (weekends don't count — `module_21_business_hours.py:78-94`) | Same email content, prefixed with "Following up on the below — let me know if you need anything from us." |
| **Day 4** | 4 business days after Day 0 | Same follow-up format again |
| **Day 6** | 6 business days after Day 0 | **Not sent to the vendor.** Internal-only: queues an email to you (Haley) with a pre-drafted GC-facing email for you to review and send yourself. The ladder stays `Active`, waiting on you — there's no automatic further action after Day 6. |

The Day-6 email to you includes the ladder record, the resolved GC recipient (COI Contact if set, else Primary Contact), and a full draft subject/body ready to copy into your own mailbox (`module_22_followup_ladder.py:598-655`). This is explicitly an interim flow — the code has a `TODO` noting a proper approval dashboard was scoped out of this build (`module_22_followup_ladder.py:566-569`).

**What stops it early:**
- **A reply from the vendor.** Any inbound email whose sender/subject/body references that ladder's named insured (including known aliases) halts the ladder (`check_response_match`, `module_22_followup_ladder.py:713-776`).
- **A new compliant certificate.** If the vendor submits an updated COI that evaluates as `Matches Requirements`, all of that vendor's active ladders are halted as Resolved (`halt_ladders_for_vendor`, `module_22_followup_ladder.py:864-873`).
- **A bounce.** First bounce on the original sender retries once to the certificate's producer/agent contact (if one is on file). A second bounce, or a bounce with no fallback contact, terminates the ladder as `Bounced-Escalated` — which, like Day 6, is a manual-review dead end, not an auto-resend (`module_22_followup_ladder.py:821-932`).

**Duplicate protection:** creating a new ladder is blocked if an `Active` ladder of the same type already exists for that vendor/client pair — this guard was added specifically after a documented incident where ~8 stuck assignments spawned 1,232 duplicate ladders before it existed (`module_22_followup_ladder.py:365-370`).

### 3b. Short-Dated Renewal ladder (policies expiring soon, independent of compliance status)

Triggered by `module_20_short_dated_check.py`, run at the end of every Module 7B evaluation for every certificate — **regardless of whether that certificate is otherwise compliant.** If any policy on the certificate has 0–30 days left before expiration, and the certificate hasn't already been flagged in the last 14 days (cooldown, `module_20_short_dated_check.py:63,142-150`), it starts the *same* Day 0/2/4/6 ladder mechanics as above, but with different wording:

> Hi,
>
> Thanks for submitting the certificate for {named insured}. We've logged it on file for {client}.
>
> Wanted to flag that the following coverage shown on this certificate expires within the next 30 days:
>
> - {Policy Type} — Policy #{number} — expires {date}
>
> Could you send the renewal certificate when it's available? Sending it now helps keep {client}'s file current without a documentation gap.
>
> Thanks,
> Carolina Compliance Solutions

The language here is deliberately locked — the code comment (`module_20_short_dated_check.py:14-17`) requires it always say "coverage shown on this certificate" / "documentation gap," never "your insurance expires" or "policy expires," to avoid implying CCS is making a coverage determination.

### 3c. Expiration reminders (separate system — no escalation, fires once per threshold)

This is *not* a ladder. `module_12_vendor_reminder_engine.py` + `module_15_email_queue_builder.py` (Part 1) send one reminder each time a policy crosses the 90/60/30/7-day-out or expired threshold (`module_12_vendor_reminder_engine.py:19-35`), and record that it fired (`Last Reminder Threshold`) so the same threshold never re-sends. There's no Day-2/4/6 follow-up on these — just one email per threshold crossed, ever, per policy.

**Important gap to know about:** the Noncompliance ladder's trigger list is `{Has Open Items, Needs Review, Missing Coverage}` (`module_7b_requirement_validator.py:773`) — it does **not** include `Expired`, which is a status `evaluate_assignment()` can genuinely return once a policy is past its grace period. In practice, an actually-expired policy is chased only by the one-shot expiration reminder above (3c), not by the persistent Day 0/2/4/6 ladder — worth knowing if you ever wonder why a clearly-lapsed vendor isn't getting repeated follow-ups.

### 3d. Cancellation / reinstatement / endorsement alerts — known broken

`module_8b.py` handles cancellation notices, reinstatements, and endorsements received from vendors (full detail: [SOP.md §2 Stage 6](SOP.md), §9 item 2). It queues internal alerts to you and vendor/agency notifications via `queue_email()` (`module_8b.py:276-301`) — but that function sets `Email Status = "Pending"` and never sets `Reminder Status`. Both current senders (`module_18`, scoped to Initial Request only, and `module_10`, which reads `Reminder Status`) will never pick these rows up. **As of this reading of the code, every cancellation/reinstatement/endorsement alert `module_8b.py` queues is stuck in Email Queue and never actually sent.** This is not fixed by the duplicate-send fix described at the top of this document — it's a separate, still-open gap. If you rely on these alerts, check Email Queue directly (filter: `Email Type` is `Cancellation Alert` / `Reinstatement Request` / `Reinstatement Alert` / `Endorsement Alert`, `Reminder Status` blank) rather than assuming they went out.

### 3e. Client-facing nag (not vendor-facing at all)

`module_19_requirements_followup.py` is unrelated to any of the above — it nags **you**, not vendors, when a new client hasn't sent you their insurance requirements yet: a Day-3 reminder and a Day-7 escalation, both queued as emails to your own inbox (`HALEY_EMAIL`), plus an annual "time to check in" nudge once a client hits their one-year anniversary. See §4 — this is the thing that's blocking a brand-new client from getting any vendor emails sent at all until you act on it.

---

## 4. Client (GC) Onboarding Checklist

From the code's perspective, here's everything that has to happen — in order — before a new client's vendors start getting chased automatically.

1. **Client pays via Stripe Checkout.** `checkout.session.completed` fires `module_16_inbound_webhook.py:215-284`, which:
   - Creates a **Clients** record with `Subscription Status = Trial` (if a coupon was applied — 30-day trial) or `Active`, `Requirements Status = "Pending — Awaiting Reply"`, and `Tier`/`Subscription Tier` resolved from the Stripe price (or amount, as fallback — `Starter`/`Growth`/`Scale` only).
   - Emails **you** a new-customer notification that explicitly says: *"Next step: Add them manually in Softr at app.carolinacompliancesolutions.com"* (`module_16_inbound_webhook.py:413`). **This Softr step is manual and not automated anywhere in this repo — UNVERIFIED what exactly needs to happen inside Softr itself (it's outside this codebase).**
   - Sends the client a single consolidated welcome email (`_send_welcome_email`, `module_16_inbound_webhook.py:443-560`) asking for exactly two things: **(1) their subcontractor list** (company, email, optional phone) and **(2) their insurance requirements** (GL/WC/Auto required-or-not and minimums, Additional Insured, Waiver of Subrogation). The email tells them COI requests start going out "within 1 business day" once you have both.

2. **You manually enter the client's insurance requirements** as rows in **Client Requirements** (Policy Type, Required, Minimum Limit, Additional Insured/Waiver/Primary Noncontributory Required). Nothing in the code creates these automatically — `module_7a_client_setup_wizard.py` would have done this but is explicitly disabled in V1 (CLAUDE.md, "Explicitly disabled in V1").

3. **You manually flip `Clients.Requirements Status` to `"Received"`.** This is a hard gate checked in two places: `module_7b_requirement_validator.py:965-970` (compliance evaluation is skipped entirely for a client until this is set) and `module_18_vendor_initial_request_sender.py:42-53` (the vendor-facing Initial Request email won't send without it, even if it's already queued). **Nothing anywhere in this codebase sets this field automatically** — confirmed by grep, the only writes to `Requirements Status` are your own manual edit and `module_19_requirements_followup.py`'s automatic transitions between `Pending — Awaiting Reply → Followed Up → Escalated` (the Day-3/Day-7 nags to yourself described in §3e), none of which ever reach `"Received"` on their own.

4. **Vendors get added to the client**, one of two ways:
   - **Bulk import:** `python bulk_vendor_import.py <client_name> <csv_file>` (CSV columns: `vendor_name,contact_name,email,phone,trade`). This enforces the client's tier cap (Starter 30 / Growth 100 / Scale 300 vendors — `bulk_vendor_import.py:27,80-113`, hard-fails the whole import rather than partially importing if it would exceed the cap), creates the Vendor record, the Vendor Client Assignment, and sets `Send Request = True` on each new vendor automatically (`bulk_vendor_import.py:147-172`).
   - **Manual entry** in Airtable — create the Vendor, link a Vendor Client Assignment to the client, and check `Send Request` yourself.

   SOP.md §9 item 6 flags a second import script, `import_vendor_roster.py`, with a broken duplicate-detection formula and no tier-cap enforcement — **use `bulk_vendor_import.py`, not that one.**

5. **Once Requirements Status = "Received" *and*** at least one vendor has `Send Request` checked, the next pipeline run (`module_17_queue_initial_requests.py` → `module_18_vendor_initial_request_sender.py`) queues and sends the Initial Request COI email to each flagged vendor, then unchecks `Send Request` (`module_17_queue_initial_requests.py:210-211`) so it doesn't re-fire. The real template that goes out (module_18's own hardcoded body always overrides whatever module_17 stored on the row — `module_18_vendor_initial_request_sender.py:175-194`):

   > Hi {vendor},
   >
   > {client} has asked us to collect a current Certificate of Insurance for your company. They use Carolina Compliance Solutions to manage subcontractor insurance compliance.
   >
   > Please email your COI to: coi@carolinacompliancesolutions.com
   >
   > In the subject line, please include: {client} — {vendor}
   >
   > Once we receive it, our system will process it automatically.
   >
   > If anything is missing or doesn't meet requirements, we'll follow up with you directly.
   >
   > When your policy renews, just send the updated certificate to the same address — no login or portal required.
   >
   > Questions? Reply to this email.
   >
   > Carolina Compliance Solutions

6. **If a vendor already had a COI on file** and the client forwarded it, it's processed the normal way through the pipeline (SOP.md §2) — no separate onboarding path needed.

**What's outside this repo entirely (UNVERIFIED):** the actual Softr portal setup for the new client (view scoping, login access), whatever Stripe dashboard configuration exists beyond what's read via webhook (coupon codes, price IDs, dunning/retry settings — see the "STRIPE DASHBOARD CONFIG" checklist at the top of `module_16_inbound_webhook.py:32-36`), and Gmail/Workspace mail routing for `coi@`. None of these are things this repo can confirm are correctly set up.

---

## 5. Weekly/Periodic Health Checks

Expands on [SOP.md §10](SOP.md#10-how-to-verify-the-pipeline-is-working), reframed as a recurring checklist rather than a one-time debug walkthrough.

**Every week (or whenever something feels off):**

- [ ] **Stuck triage.** In **Incoming Documents**, look for rows with `Status = "Failed"` or `"Pending Internal Review"` older than 48 hours. Anything past 48 hours *should* have auto-escalated to `"Escalated"` and emailed you (`operational_email_handler.py:126-177`, `daily_cron.py:30-33`) — if you're seeing old `Pending Internal Review` rows that never escalated, that's worth investigating directly rather than assuming the alert fired.
- [ ] **Stuck extractions.** In **Incoming Extractions**, check for `Processing Status = "Unmatched"` (no vendor found at all — distinct from `"Needs Review"`, see SOP.md §2 Stage 4) sitting for more than a few days. These need a manual vendor match.
- [ ] **Stuck emails.** In **Email Queue**, filter `Reminder Status = "Failed"` — nothing automatically retries these (SOP.md §4), so any row here needs a manual look and a manual reset of the status to re-queue it.
- [ ] **Orphaned alert emails (known gap — see §3d).** In **Email Queue**, filter `Email Type` in `{Cancellation Alert, Reinstatement Request, Reinstatement Alert, Endorsement Alert}` with a blank `Reminder Status`. These are permanently stuck under the current code — this is the one queue you should expect to have to clear manually until it's fixed.
- [ ] **Ladders parked at Day 6.** In **Outbound Follow-Ups**, filter `Stage = "Day 6 Escalated"` and `Status = "Active"`. Each of these is waiting on you to send (or decline to send) the GC-facing email that was drafted and emailed to you — the ladder does not resolve itself.
- [ ] **Ladders parked at Bounced-Escalated.** Same table, `Stage = "Bounced-Escalated"` — means an email genuinely couldn't be delivered anywhere and needs a manual contact-info fix.
- [ ] **Requirements backlog.** In **Clients**, filter `Requirements Status` in `{Pending — Awaiting Reply, Followed Up, Escalated}`. Anything here means that client's vendors are getting zero automated activity — no Initial Requests, no compliance evaluation — until you set their requirements and flip the status to `Received` (§4, step 3).
- [ ] **Exceptions expiring.** In **Vendor Requirement Overrides**, `Exception Status = "Expiring Soon"` — the system flags these automatically (`exception_manager.py:214-224`) but the email alert for this is currently disabled in code (`exception_manager.py:227-241`, commented out as "post-launch feature") — so this table is the only place you'll see it; nothing will email you.

**Weekly (fixed schedule):**

- [ ] **Backup ran.** `airtable_backup.py` is intended to run Sundays ~6am UTC per `railway.toml:19`'s comment (again, confirm the live schedule in Railway) and exports 11 core tables to Cloudflare R2. Check the R2 bucket (Cloudflare dashboard, or `verify_r2_upload.py` if you have shell access) for a file dated after the most recent Sunday.

**Occasionally (sanity check the pipeline itself is actually running):**

- [ ] Confirm the pipeline is actually firing on the cadence you expect — the fastest signal is a fresh `Incoming Documents` row appearing within ~15–30 minutes of you sending a real test COI to `coi@carolinacompliancesolutions.com` (full walkthrough: SOP.md §10, steps 1-8). If nothing shows up, check Railway's cron logs directly — this repo can't tell you whether the cron actually fired.

---

## 6. What To Do When Something Looks Wrong

A quick symptom-to-place-to-look index. Once you know which table/module is involved, SOP.md has the exact field-by-field technical detail — this section only tells you where to start looking.

| Symptom | Check first | Cross-reference |
|---|---|---|
| **"A vendor says they never got an email"** | **Email Queue** — filter by their email, check `Reminder Status`/`Sent At`. Then **Outbound Follow-Ups** to see if a ladder exists for them and what `Stage` it's at (maybe it's genuinely still Day 0/queued, or it bounced). | SOP.md §4; §3 above for ladder mechanics |
| **"A cert isn't showing up" (vendor says they sent it)** | **Incoming Documents** first (did the email even arrive/get logged, and what `Status`?), then **Incoming Extractions** (`Processing Status` — `Low Confidence`, `Duplicate`, `Needs Review`, or `Unmatched` all mean a human needs to act, not that it failed silently). | SOP.md §2, Stages 1–4 |
| **"Duplicate email complaint"** | Check whether the vendor actually got two *different* emails from two *different* systems (e.g. an Initial Request from module_17/18 *and* a separate Noncompliance ladder Day-0 from module_7b/22 — both are legitimate, independent triggers) vs. a true duplicate. The old module_17/module_10 double-send bug (SOP.md §9 item 1) is fixed as of this reading — see the note at the top of this document — so a true duplicate today is more likely a new issue worth investigating fresh. | Top of this doc; SOP.md §4 |
| **"Client says they signed up but nothing's happening for their subs"** | **Clients** table — check `Requirements Status` (must be `"Received"`, and nothing sets this but you — §4 step 3) and confirm **Client Requirements** rows actually exist for them. Then **Vendors** — is `Send Request` checked on the subs that should get an Initial Request? | §4 above |
| **"Vendor was marked non-compliant and I don't think that's fair"** | **Compliance Log** — full audit trail of every decision and the reasons behind it, one row per evaluation. Check **Vendor Requirement Overrides** to see if an exception should be (or already was) approved. | SOP.md §2 Stage 7, §7 (data model), §9 |
| **"A cancellation/reinstatement/endorsement notice came in — did anyone get told?"** | **Email Queue**, filter those four `Email Type` values with blank `Reminder Status` — these are currently stuck and not actually sent (§3d, known open gap). Don't assume silence means nothing happened; check the queue directly. | §3d above; SOP.md §9 item 2 |
| **"A follow-up ladder seems stuck / stopped progressing"** | **Outbound Follow-Ups** — check `Stage` and `Status`. `Day 6 Escalated` and `Bounced-Escalated` are both intentional stopping points waiting on you, not bugs. Anything else stuck for multiple days may mean the `railway_cron_followup.py` cron isn't running — check Railway directly. | §3a above; SOP.md §3, §4 |
| **"Is the pipeline even running?"** | No table shows this directly — check Railway's cron service logs. The best indirect signal is sending yourself a real test COI and watching for a new **Incoming Documents** row within the expected window. | §5 above; SOP.md §10 |
