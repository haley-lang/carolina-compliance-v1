# MARKETING_BUILD_PLAN.md

Build plan for the **Marketing** tab ("Marketing" is a working name; it is one nav label and easy to rename).
Written from the real code. Read CLAUDE.md first. Build in stages, in order. Do not build everything in one pass.

## Goal
Turn Haley's everyday work in the review portal into marketing that drives results: content ideas, drafts, a results scoreboard,
and "what to try next". Brand feel: small-town, warm, a real person who is also sharp and capable.
The agents READ and DRAFT only. They never post, send, or change client data. Haley approves everything.

## Where it fits (what already exists)
- review_app.py (Flask) already has Pulse, Queue, Gaps, Requests, Audits, Corrections, Emails. Nav lives in templates/queue.html (topnav-links); other templates repeat it.
- Corrections table (CORRECTIONS_TABLE_ID) already stores every field change plus "Why (Haley's note)". This is the best raw material for stories.
- Claude API is already used in review_app.py (Haiku for _draft_prompt_rule). Reuse that pattern.
- pulse_cron.py shows the cron pattern (Railway cron service). Marketing jobs follow it.
- CLAUDE.md rule: all persistent state in Airtable. Do NOT put marketing data in SQLite. Create new Airtable tables for it.
- Infrastructure rule: production-grade only. Nothing depends on Haley's Mac.

## Hard rules (every stage, enforced in code and tests)
1. Draft-only. No sending, posting, or emailing from Marketing. No SendGrid calls.
2. Banned words in any generated copy: compliant, verified, covered, certified. Use "on file / not on file". Build one shared compliance_guard function and test it. Every draft passes through it before it is shown.
3. Never claim auditors ask for every policy term or past years (audits typically look at the most recent year).
4. No state-specific legal or audit-rule claims. Audience is all states. Keep rules general.
5. Anonymize by default. The story spotter may read real client and vendor names internally; every draft strips names, policy numbers, carriers, and addresses unless Haley approves naming someone. Billy/RTR needs a "needs Billy's okay" flag per piece.
6. Honest numbers. Say "on file", never "percent compliant". Label results from one pilot client as a pilot result. Never imply a typical outcome.
7. Positioning: CCS is a tool that streamlines the process; anything below the confidence cutoff goes to human review. Do not say the business relies on AI.
8. Voice: professional but friendly, plain-spoken, short sentences, no hype. Include the website link and a low-pressure "happy to chat" when mentioning CCS in comments and DMs.
9. Match the existing portal look (static/review.css): warm, approachable, not scary or black and white.

## Airtable tables to create (use direct Airtable tool access; the API key lacks schema write)
- Marketing Signals: source (correction / gap / audit / milestone / request reply), summary (anonymized), raw reference id, created at.
- Marketing Ideas: idea, why it is a story, audience (GC / Insurance agent), topic (Audits / GL / Workers' comp / Certificate mistakes / Behind the build / DCLI experience), status (New / Approved / Dismissed).
- Content Queue: idea link, platform (TikTok / Facebook / LinkedIn / Email / Website FAQ), draft text, hooks, status (Draft / Approved / Posted), needs Billy's okay (checkbox), compliance guard result, tracked link.
- Results Log: content link, views, comments, messages, conversations, sign-ups, notes.
- Conversations: person or company (link to Prospects when possible), source content, what happened, next step.
- Playbook: lessons learned (what worked, hook styles that win, Haley's edit reasons).

## Stages

### Stage 0: foundation
- compliance_guard.py + tests (banned words, audit-claim phrases, state-specific claim patterns).
- anonymizer helper + tests.
- Create the Airtable tables above. Add "Marketing" to the nav (all templates) and a /marketing page skeleton behind login_required.
- Tracked-link builder: one unique UTM link per content piece for the website (including the demo page and Audit Prep page).
Done when: tests pass, page loads, nothing else in the portal changed.

### Stage 1: signals and story spotter (read-only)
- marketing_signals.py reads existing activity: new Corrections rows and Haley's notes, repeated gaps found across certificates, audit work, caught expirations, milestone moments (for example a client's first time with every certificate on file).
- Story-spotter job (cron, follows pulse_cron.py) asks Claude to turn signals into anonymized story ideas -> Marketing Ideas.
- /marketing shows the ideas feed with Approve / Dismiss, plus a small quick-note box for stray thoughts.
Done when: real Corrections data produces sensible anonymized ideas and every one passed the guard.

### Stage 2: one event, a full content set
- From an approved idea, generate: TikTok/Facebook talking points (3-4 bullets, not a full script), a LinkedIn post, a Facebook post, a short prospect email draft, a website FAQ line. 3 hook options each.
- Everything lands in Content Queue as Draft with the guard result shown.
Done when: one real idea produces a full set that passes the guard.

### Stage 3: results and links
- Results Log entry form on each Posted piece. Conversations list tied to Prospects.
- Scoreboard (on-file numbers only, honest sample-size labels).
- Google Analytics 4 feed: Squarespace has no traffic API, so connect GA4 to Squarespace (measurement ID in Squarespace Developer Tools > External API keys), then pull from the GA4 Data API in a cron job into Airtable. Requires Haley to complete the Squarespace step and grant GA4 access.
Done when: a tracked link click shows against its content piece.

### Stage 4: the plays
- "On File or Not On File?" series: recreated example certificates (never real ones) with a reveal, built from real gap patterns.
- Audit-notice kit: checklist for GCs and a referral one-pager for insurance agents (build on the existing Partner Program PDFs).
- Milestone moments: draft a personal thank-you and referral ask in Haley's voice for her approval.
- Winners to ads: when a post outperforms, draft follow-ups and a suggested small ad budget. Suggestion only.
- Reply drafts for comments and DMs in Haley's style; answer-desk drafts (helpful, no pitch, 9:1 rule).

### Stage 5: learning
- Edit log: when Haley changes a draft, ask "what was off?" in one line, save before/after to Playbook.
- Hook lab and experiment planner read results, update the Playbook, and propose the next small test.
- Feed Playbook lessons into the generation prompts.

## After each stage
Run the full test suite (.venv/bin/python -m pytest tests/). Add tests for new code. Present what was done in plain language.
Stop and ask Haley only when: a step needs her accounts (GA4, Squarespace), a deploy to Railway, or a product decision.
