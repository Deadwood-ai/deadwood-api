# Operator Data Coverage

Use this checklist with the platform status check. Query the previous 24 hours
first; use 30 days for trends or sparse publication activity. Report inaccessible
data as unknown, not zero. Keep each result to counts, oldest outstanding time,
and a few anomalous IDs.

## Connection And Permissions

For routine production queries, follow the target, identity and transaction
checks in [trusted analyst access](analyst-database-access.md#routine-production-reads).
That route uses `team_analyst` and supports authorized detail investigations.
A locally running connector or a local `.env` does not prove which database or
role it uses. Inspect the configured endpoint without printing secrets.

The separate `deadtrees_operator_status` role serves existing monitoring probes.
Its column-limited contract is documented below; those exclusions are not the
analyst contract. Both routes require verified access on the execution host.
Do not substitute a privileged connection when access fails.

Use explicit columns for column-granted tables. Check `has_column_privilege`
when `has_table_privilege(..., 'SELECT')` is false. Table grants and RLS are
separate checks. A reviewed migration alone does not establish live permissions.

## Coverage

| Area | Tables | Signals |
| --- | --- | --- |
| Input and processing | `v2_datasets`, `v2_statuses`, `v2_queue`, `v2_logs` | New uploads, completed processing, failures, active workers, waiting age and progress |
| Outputs | `v2_metadata`, `v2_raw_images`, `v2_orthos`, `v2_orthos_processed`, `v2_cogs`, `v2_thumbnails`, `v2_model_preferences` | Missing outputs, size/runtime anomalies, model-preference update freshness |
| FreiDATA | `data_publication`, `jt_data_publication_datasets` | Requests created/published in 24h and 30d; states; oldest pending; linked dataset counts |
| Trust | `dataset_audit`, `dataset_flags`, `dataset_flag_status_history`, `reference_datasets`, `reference_patches` | Audits, unresolved flags, review progress and export readiness |
| Reuse | `prepackaged_dataset_definitions`, `prepackaged_dataset_versions`, `prepackaged_dataset_download_grants` | Build/publish freshness, package failures, grant issuance/validation/expiry |
| Delivery | `processing_notification_events`, `user_notification_preferences` | Pending/error deliveries, attempts, overdue retries, sent counts |
| Field work | `priwa_projects`, `priwa_project_flights`, `priwa_befallsgruppen`, `priwa_befallsgruppe_flights`, `priwa_kaeferbaeume`, `public_tree_observations` | New/updated/deleted observations and flight activity |
| Warning maps | `priwa_warnkarte_versions`, `priwa_warnkarte_publications`, `priwa_warnkarte_archive_events` | New versions, publication and archive changes |
| Search | `v2_search_queries` | Request counts; correlate failures with PostHog |

Download validation is not proof of completed byte transfer. The Zulip daily
summary's processed count concerns newly created datasets without `has_error` and with any of
`is_deadwood_done`, `is_forest_cover_done`, `is_combined_model_done`, or
`is_odm_done` true; it does not count every older queued dataset completed that day. Label the
cohort and avoid using status `updated_at` alone as a completion timestamp.

## FreiDATA Scheduler

Start with database state, then inspect the actual cron entry and bounded fresh
logs on the configured host. The script is `scripts/freidata_cron.sh`; resolve
its log path from the invocation/configuration rather than assuming `/data/logs`.
Check scheduler startup failures as well as pipeline logs. A scheduled entry
alone is not evidence of a successful run.

- `pending`: not yet processed by the publisher; flag requests older than 24h.
- `uploading`: inspect log/file progress and record ID before calling it stuck.
- `in_review`: check the linked FreiDATA community review; manual approval may
  be required. It is not an upload failure.
- `error` or `declined`: inspect the relevant bounded error/review evidence.
- `published`: report DOI and `published_at`; historical backfills may share a
  timestamp, so do not infer historical daily throughput without corroboration.

Zulip's `New Data Publications` topic is corroboration, not the source of truth.
No messages does not imply no requests. `notified_at` alone does not prove a
specific Zulip lifecycle message was sent. Monitoring never invokes `cron`,
`publish`, or `sync`: these commands mutate data and may submit packages.

## Dedicated monitor: column-limited reads

This section and its privilege expectations apply to `deadtrees_operator_status`,
not `team_analyst`. The analyst allowlist is in the
[analyst access playbook](analyst-database-access.md).

The monitor metadata grants use explicit columns. The allowlist is in
[`20260905160000_operator_monitoring_access.sql`](../../supabase/migrations/20260905160000_operator_monitoring_access.sql).
Use IDs, lifecycle states, timestamps, counts, sizes and runtime measurements.
Output JSON, manifests, author/contact fields, free-text audit notes, field
geometry, observer names, recipient emails, token hashes and raw payloads are
excluded. No grants are added to Auth, user profiles, project memberships,
newsletter contacts, prediction geometries or user-info publication joins.
A missing grant on those tables is intentional.

Existing production grants on `v2_logs`, `v2_queue`, and `v2_statuses` are
preserved. This migration does not remove existing log/error-text access or
harden inherited privileges. Check effective privileges and role membership on
the actual monitoring connection before claiming that its whole access boundary
matches a clean install. It must not be a superuser, have `BYPASSRLS`, or inherit
app/admin roles. Do not silently change role attributes during a status check.

```sql
select session_user, current_user, current_database();
select rolsuper, rolbypassrls from pg_roles where rolname = current_user;
select
  has_table_privilege(current_user, 'public.processing_notification_events', 'SELECT') as whole_table,
  has_column_privilege(current_user, 'public.processing_notification_events', 'status', 'SELECT') as status_column,
  has_column_privilege(current_user, 'public.processing_notification_events', 'recipient_email', 'SELECT') as excluded_email;
-- Clean install expects false, true, false. SELECT * is not supported here.
select status, count(*), max(delivery_attempts) as max_attempts,
  min(next_attempt_at) filter (where next_attempt_at <= now()) as oldest_due,
  max(updated_at) as latest_update, max(sent_at) as latest_sent
from public.processing_notification_events
group by status;
```

RLS permits the dedicated monitor to count metadata across private datasets and
all PRIWA projects, including soft-deleted observations. This is deliberate
platform-wide operator access, not a public-data-only view. App roles receive
neither the monitoring role nor its policy. Existing app-user RLS is unchanged.

## FreiDATA aggregates

Use this query alongside the processing aggregates in the platform status
playbook. It counts packages before joining dataset links so packages with many
datasets are not counted repeatedly. The state rows include current backlog;
the 24h/30d counters use request creation and recorded publication timestamps.

```sql
with links as (
  select publication_id, count(*) as datasets
  from public.jt_data_publication_datasets group by publication_id
)
select p.status, count(*) as packages,
  count(*) filter (where p.created_at >= now() - interval '24 hours') as new_24h,
  count(*) filter (where p.created_at >= now() - interval '30 days') as new_30d,
  count(*) filter (where p.published_at >= now() - interval '24 hours') as published_24h,
  count(*) filter (where p.published_at >= now() - interval '30 days') as published_30d,
  min(p.created_at) as oldest_request,
  max(p.created_at) as latest_request,
  max(p.published_at) as latest_published,
  extract(epoch from now() - min(p.created_at)) / 3600 as oldest_request_age_hours,
  coalesce(sum(links.datasets), 0) as linked_datasets
from public.data_publication p
left join links on links.publication_id = p.id
group by p.status order by p.status;
```

Report absent states as zero only after this query succeeds. Count
`pending`, `uploading`, `in_review`, `error`, `declined`, and `published`
separately, and preserve unexpected or null states for investigation. Age is
request age, not time in the current state. There is no state-transition time
in this table. A successful query proves database coverage, not scheduler health.

The compact `scripts/operator_status.py` snapshot still covers core processing,
API and host checks. Run these additional SQL and host checks during the Operator
Chat pass; a core-script green result alone does not cover these workflows.

## Other workflow checks

- Audit/flags: count audits by `audit_date`, flags by current status and
  `created_at`/`updated_at`, and history by `changed_at`. Inspect oldest unresolved
  flags; free-text reasons require a separate authorized investigation.
- Downloads: group versions by definition/status; compare latest `built_at` and
  `published_at`, dataset/artifact counts and sizes. `draft` alone does not prove
  a failed build; corroborate with bounded build logs. Grant issuance/validation
  counts show access attempts, not completed transfers.
- Delivery: distinguish pending, sending, failed, skipped and sent. Report
  overdue `next_attempt_at`, maximum attempts and oldest update; do not fetch
  recipient emails or raw status snapshots.
- PRIWA: count created/updated/deleted observations by project, reviewed flights
  by `reviewed_at`, group/link changes by their timestamps, warning-map imports
  by `imported_at`, publications by `published_at`, archive/restore events by
  `acted_at`. Hard deletions cannot be reconstructed from current rows.

Save compact counts and timestamps in the existing ignored operator state.
Compare with the prior snapshot to report what changed, succeeded, remains
stuck, or needs attention. Freshness thresholds for package builds depend on the
release cadence; do not label an unchanged published package as failed merely
because it is old. Missing host/log access remains an explicit coverage gap.
