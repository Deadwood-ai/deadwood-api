# Operator Chat

Use this playbook for the DeadTrees main operator thread: monitoring,
prioritisation, and handoff. The operator thread stays read-only by default and
does not implement fixes directly.

## Contract

- Run from `main`.
- Start every refresh with `git fetch origin --prune`, then
  `git status --branch`.
- If the tree is clean and behind `origin/main`, pull with `git pull --ff-only`.
- If the tree is dirty, do not pull or overwrite local changes until the changed
  files are understood.
- Prefer read-only checks. Do not mutate production, post messages, update
  Linear, commit, push, or open PRs unless the user explicitly asks.
- Create Linear issues or start worker threads only after a concrete signal:
  failing platform check, repeated anomaly, user request, security alert,
  stale project-management item, or missing owner for important work.
- Keep implementation work out of this thread. Start a fresh thread/worktree
  from a compact handoff when code or production changes are needed.

## Source Docs

Local source of truth:

- [`platform-status-check.md`](platform-status-check.md) for DB, API, storage,
  processing, backups, PostHog, and Zulip platform health.
- [`dataset-debugging.md`](dataset-debugging.md) for dataset-specific failures.
- [`operator-data-coverage.md`](operator-data-coverage.md) for monitoring grants,
  FreiDATA scheduler checks, and product workflow activity beyond processing.
- [`../analytics/deadtrees-data-factory.md`](../analytics/deadtrees-data-factory.md)
  for the product operating model.
- [`../analytics/aarrr-framework.md`](../analytics/aarrr-framework.md) for the
  current PostHog event map and dashboard IDs.
- [`../create-linear-issue.md`](../create-linear-issue.md) and
  [`../agents/rules.md`](../agents/rules.md) for Linear conventions.

3Dtrees operator references may be useful when available, but are not tracked in
this repo at the time this playbook was added:

- `linear-triage-workflow.md`
- `upload-anomaly-linear-bot.md`

## Cadence

### Monitoring Boundary

Codex operator threads are not continuous monitors. Treat this playbook as the
procedure for a manual refresh, a scheduled wake-up, or an explicitly delegated
check. Do not assume the main operator thread, a worker thread, or a status-check
thread is always running in the background.

For backups, use two independent signals:

- freshness checks from `scripts/operator_status.py` when an operator check runs;
- backup-server email alerts from Gmail/Zulip intake when a scheduled check,
  weekly review, or incident follow-up runs.

If backup freshness has not been checked in the current operator window, report it
as skipped or stale evidence instead of inferring health from a previous Codex
thread.

### Manual Status Refresh

Start a manual operator refresh with the repo-local compact status script:

```bash
python3 scripts/operator_status.py --write-state --format markdown
```

The script is the cheap infrastructure spine for the operator chat. It checks
repo state, API health, optional database aggregates, optional host/storage
probes, optional backup freshness, and writes ignored compact state to:

- `.local/operator/operator-state.json`
- `.local/operator/operator-latest.md`

Production-only checks are opt-in through local environment variables so the
tracked script contains no credentials:

- `DEADTREES_OPERATOR_DATABASE_URL` for the existing purpose-specific monitor
  `psql` summary; this is separate from the routine analyst connection.
- `DEADTREES_OPERATOR_PROCESSING_HOST` for processing host disk/heartbeat probes.
- `DEADTREES_OPERATOR_STORAGE_HOST` for storage host disk probes.
- `DEADTREES_OPERATOR_BACKUP_HOST` plus either `DEADTREES_OPERATOR_BACKUP_PATH`
  or `DEADTREES_OPERATOR_BACKUP_COMMAND` for backup freshness.
  On the Freiburg network/VPN, prefer:

  ```bash
  DEADTREES_OPERATOR_BACKUP_HOST=remote-backup@dtbackup \
    python3 scripts/operator_status.py --write-state --format markdown
  ```

After reading the script output, use connector checks for the surfaces that
cannot be reliably accessed by repo-local tooling: PostHog, Linear, Gmail, and
Zulip. Keep those checks delta-first and inspect detail only after a concrete
signal appears.

For product/factory status, run the data-factory scorecard after the platform
snapshot:

```bash
python3 scripts/data_factory_scorecard.py --write-state --format markdown
```

Both scripts use `psql` for their optional monitor DB probe. They do not load
`DEADTREES_ANALYST_DATABASE_URL`, verify analyst identity or start an explicit
read-only transaction. Preserve existing monitor configuration; do not substitute
the analyst URI into `DEADTREES_OPERATOR_DATABASE_URL`.

For routine production SQL, use the verified transaction in
[trusted analyst access](analyst-database-access.md#routine-production-reads).
The scorecard's query can be printed without connecting, then executed inside
that transaction in place of the sample dataset query:

```bash
python3 scripts/data_factory_scorecard.py --print-sql
```

PostHog remains a connector-side add-on for frontend pain and funnel signals.
Print the compact PostHog SQL templates with:

```bash
python3 scripts/data_factory_scorecard.py --posthog-sql
```

### Hourly Micro-Check

Use this for cheap, token-efficient monitoring. Report only deltas and
exceptions unless the user asks for detail.

1. Repo state: branch, dirty files, local/remote divergence.
2. API health: public OpenAPI route or health endpoint.
3. Database anomaly summary: aggregate dataset/status/queue/log counts.
4. Worker/server heartbeat: latest processing activity, queue age, disk pressure,
   and stuck non-idle work.
5. Storage and backup freshness when local host probes are configured.
6. PostHog: exact counts for verified events in the check window, plus cheap
   frustration indicators such as exceptions, rage clicks, dead clicks, and
   route-level drop-off deltas.
7. Linear: recent platform issues, stale urgent triage, repeated anomaly
   fingerprints, and active blockers.
8. Gmail/Zulip: urgent delta only, using bounded search or monitored unread
   checks.

Do not inspect PostHog recordings in the operator chat by default. The operator
chat should identify candidate recording IDs or frustration clusters. Start a
worker thread for recording review only when a concrete frontend pain signal is
visible.

### Daily Full Check

Run the normal 24-36 hour platform status playbook from
[`platform-status-check.md`](platform-status-check.md). Add host disk/container,
backup/export freshness, and frontend smoke checks when symptoms or recent
changes make them relevant.

### Weekly Operator Review

Produce a broader operating review:

- data-factory throughput and current product constraint
- Linear drift and project priority alignment
- docs drift or missing runbook coverage
- recurring support themes from Gmail/Zulip/Linear
- candidate issues and candidate worker threads

## Token-Efficient State

Use local ignored state for cursors and compact summaries. Recommended paths:

- `.local/operator/operator-state.json`
- `.local/operator/operator-latest.md`

Store only:

- last check time and window
- last seen Gmail and Zulip IDs/cursors
- relevant Linear issue IDs
- PostHog event-count baselines
- compact infrastructure snapshot from `scripts/operator_status.py`
- last verdict and top risks

Do not store secrets, raw logs, bearer tokens, database URLs, complete email
bodies, large PostHog payloads, or full message history.

## Data-Factory Health Model

Use [`../analytics/deadtrees-data-factory.md`](../analytics/deadtrees-data-factory.md)
as the product source of truth.

- Acquisition: homepage, map, archive, waitlist/contact, and sign-up intent.
- Activation: upload started/completed, processing completed, and the owner
  opens the processed result.
- Trust: metadata quality, processing reliability, viewer availability, audit
  state, and reference/correction progress.
- Impact: views, downloads, release/reuse signals, citation/publication signals,
  and partner follow-up.

Important caveat: current PostHog events are incomplete. Missing events are an
instrumentation gap until corroborated by DB/API/user-facing evidence; do not
treat missing analytics events alone as proof that the product is broken.

## Linear Drift Checks

Keep these read-only unless asked to update Linear.

- Triage issues older than the threshold used in the report.
- Urgent or high-priority issues without owner, next action, or recent update.
- Repeated bot fingerprints without a consolidated RCA or owner.
- `Todo` or `Backlog` buckets that are too large or stale to guide work.
- `In Progress` issues without recent activity.
- Merged PRs whose linked issues are not `Done`.
- Issues no longer aligned with the current data-factory bottleneck.

Before creating a new issue, search Linear for similar dataset IDs, error
fingerprints, user-visible symptoms, and Zulip/email context. Agent-created
issues start in `Triage`, stay unassigned unless the user decides otherwise, and
use labels such as `Bug`, `Improvement`, `Needs RCA`, and
`Needs User Notification` when appropriate.

## Gmail And Zulip Intake

Start count-first and inspect only likely actionable messages.

Suggested Gmail terms:

```text
3Dtrees OR 3dtrees.earth OR DeadTrees OR deadtrees.earth OR dataset OR upload OR Galaxy OR download OR access OR error OR bug OR processing
```

Use Gmail search filters such as:

```text
newer_than:7d -in:spam -in:trash -category:promotions -category:social
```

For Zulip, prefer monitored unread checks for scheduled deltas. Add a
recent-history pass after downtime or when project context matters, because
unread-only checks can miss active project discussion.

Do not post to Zulip, send email, or mark messages read as a side effect unless
the user explicitly asks.

## Handoff Protocol

Every operator report should be compact:

- verdict: `green`, `yellow`, or `red`
- product constraint: the current weakest data-factory step
- top risks: no more than three
- candidate issues: with evidence and suggested labels
- candidate worker threads: with exact handoff context
- skipped or blocked surfaces

When starting a worker thread, pass only the minimum useful handoff:

- issue or signal URL/ID
- exact user-visible symptom
- relevant dataset IDs or fingerprints
- evidence checked and evidence still missing
- safe validation path
- production mutation boundary
