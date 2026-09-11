# Platform Status Check

Use this playbook for daily or ad-hoc DeadTrees status checks. It is safe to
track and intentionally contains no credentials.

## Operating Mode

- Start cheap and aggregate first, then drill down only on anomalies.
- Use the last 24 hours by default unless the user specifies another window.
- Use UTC for database/server queries and write the final summary in Berlin time
  unless the audience needs UTC.
- Do not paste secrets, bearer tokens, database URLs, or passwords into the report.
- For access paths, use local-only `docs/ops/local-access.md` and
  `.codex/local-access.md` when present.

## Surfaces

Check the surfaces relevant to the question:

- production database through [trusted analyst access](analyst-database-access.md)
- processing queue, `v2_statuses`, `v2_logs`, and recent failures
- storage/API server health and public API contract
- processing server/container heartbeat when host access is needed
- frontend smoke behavior when user-visible symptoms are possible
- PostHog traffic, exceptions, uploads, downloads, and audit events
- Gmail backup alerts, Zulip reports, and automation summaries when relevant
- reference exports and backups for data freshness incidents

## Operator Chat Integration

This playbook covers platform health only: database, API, storage, processing,
PostHog, Zulip, exports, and backups. The broader Operator Chat workflow in
[`operator-chat.md`](operator-chat.md) wraps this with product-health,
project-management, Gmail, and handoff checks. Use this playbook as the daily
full platform check inside that broader operator cadence.

## Fast Preflight

0. Generate the compact operator snapshot when running from the repo:

   ```bash
   python3 scripts/operator_status.py --write-state --format markdown
   ```

   Use this as the first-pass summary, then drill down only into warnings,
   failures, skipped surfaces, or user-visible symptoms. Its optional database
   probe uses the separate monitor connection described in
   [Operator Chat](operator-chat.md#manual-status-refresh); it does not load or
   verify analyst access.

   To include backup freshness when connected to the university network or VPN,
   point the backup probe at the backup user. The script uses the documented
   Borgmatic path on `dtbackup`, lists the latest database and storage archives,
   and warns when parsed archive timestamps are older than 36 hours:

   ```bash
   DEADTREES_OPERATOR_BACKUP_HOST=remote-backup@dtbackup \
     python3 scripts/operator_status.py --write-state --format markdown
   ```

   When checking backups, also inspect backup-server email alerts for the same
   operator window, or for the last two months during weekly/full reviews:

   ```text
   in:anywhere from:dtbackup@gmx.de after:YYYY/MM/DD before:YYYY/MM/DD
   ```

   Treat the backup surface as fully checked only when both signals are covered:
   latest Borg archives are fresh, and recent alert emails contain no unresolved
   `URGENT`, degraded, failed-device, rebuild, or Borgmatic failure signal.

   If the backup probe fails through `dtbackup`, distinguish host reachability
   from backup-tool access:

   ```bash
   ssh -o BatchMode=yes -o ConnectTimeout=5 dtbackup 'hostname'
   ssh -o BatchMode=yes -o ConnectTimeout=5 remote-backup@dtbackup \
     'set -o pipefail; /home/remote-backup/.local/bin/borgmatic --config /home/remote-backup/.config/borgmatic/database_dump_direct.yaml list \
      | awk '\''/^[[:alnum:]_.-]+-[0-9]{4}-[0-9]{2}-[0-9]{2}T/ && $1 !~ /[.]checkpoint([.][0-9]+)?$/ { latest=$0 } END { print latest }'\'''
   ```

   A checkpoint archive is incomplete and must never be used as freshness
   evidence; the fallback prints the newest completed archive only.

1. Confirm local access files exist if host or MCP access is needed:

   ```bash
   test -f .codex/config.toml
   test -f .codex/local-access.md
   test -d docs/ops
   ```

2. Run the [analyst preflight](analyst-database-access.md#routine-production-reads)
   before production SQL. It verifies database, login, role membership and
   transaction read-only mode. Local access files alone do not establish access;
   report a failed or unavailable connection as unknown, not zero.

3. Smoke the public API:

   ```bash
   curl -fsS https://data2.deadtrees.earth/api/v1/openapi.json | jq '.openapi'
   ```

4. If host checks are needed and approved, use compact SSH probes:

   ```bash
   ssh -o BatchMode=yes -o ConnectTimeout=5 storage-server 'hostname'
   ssh -o BatchMode=yes -o ConnectTimeout=5 processing-server 'hostname'
   ssh -o BatchMode=yes -o ConnectTimeout=5 remote-backup@dtbackup 'hostname'
   ```

## Database Queries

For every full check, include the product workflows in
[`operator-data-coverage.md`](operator-data-coverage.md): FreiDATA publication,
audits/flags, output completeness, archive downloads, notification delivery,
and field activity. Verify role/column access first; missing permissions are
coverage gaps, never zero activity. Include publication counts and scheduler
health in the compact report even when the Zulip publication topic is quiet.

Prefer aggregate counts before row-level inspection:

```sql
select
	count(*) filter (where created_at >= now() - interval '24 hours') as datasets_created_24h,
	count(*) filter (where updated_at >= now() - interval '24 hours' and has_error) as statuses_error_24h,
	count(*) filter (where current_status <> 'idle') as non_idle_now
from v2_statuses;
```

```sql
select is_processing, priority, count(*), min(created_at) as oldest, max(created_at) as newest
from v2_queue
group by is_processing, priority
order by is_processing desc, priority desc;
```

```sql
select level, count(*)
from v2_logs
where created_at >= now() - interval '24 hours'
group by level
order by count(*) desc;
```

Then inspect specific fingerprints only when counts look wrong.

## Host Checks

Use compact one-shot commands and keep output capped:

```bash
docker inspect deadtrees-processor-1 \
  --format 'StartedAt={{.State.StartedAt}} RestartCount={{.RestartCount}} OOMKilled={{.State.OOMKilled}} ExitCode={{.State.ExitCode}}'
df -h / /data
tail -n 80 /data/logs/reference_export.log
tail -n 80 /data/logs/api_container.log
```

If processing-server SSH is blocked, use database logs as primary evidence and
state that host-level corroboration was unavailable.

## Backup Checks

Backup freshness is not continuously monitored by a Codex thread. During daily
or weekly operator checks, combine the live Borg freshness probe with a bounded
Gmail alert search. See [`database-backups.md`](database-backups.md) for the
backup architecture, success contract, validation, and rollback procedures.

Checklist:

1. Run the Borg freshness probe with `DEADTREES_OPERATOR_BACKUP_HOST=remote-backup@dtbackup`.
2. Search backup-server alert email from `dtbackup@gmx.de` for the check window.
3. If any alert mentions degraded RAID, failed devices, rebuild activity, or
   Borgmatic/backup failures, verify current host state and current archives
   before reporting backups as healthy.

Use this two-month audit query when checking whether backup-server alerting has
reported bigger issues:

```text
in:anywhere from:dtbackup@gmx.de after:YYYY/MM/DD before:YYYY/MM/DD
```

Then narrow likely incidents with:

```text
in:anywhere from:dtbackup@gmx.de after:YYYY/MM/DD before:YYYY/MM/DD (URGENT OR Failed OR Degraded OR Rebuild OR "Device Failed" OR "Time Out Error")
in:anywhere after:YYYY/MM/DD before:YYYY/MM/DD (borgmatic OR "remote-backup" OR database_dump OR pg_dump) (failed OR failure OR error OR warning OR cron)
```

Interpretation guide:

- `RaidAlert-URGENT`, `Volume Degraded`, `RaidSet Degraded`, or `Device Failed`
  is a real storage incident until host-side state proves recovery.
- `Start Rebuilding`, `HotSpare Used`, and `Complete Rebuild` describe the
  recovery sequence; confirm current `/proc/mdstat`, `/mnt/raid` mount, and
  latest Borg archives before closing the incident.
- monthly `raid_scrub_check.sh` cron emails that only say scrubs started are
  informational unless followed by error output or RAID alerts.

## Final Report

Include:

- overall status: green, yellow, or red
- window checked with concrete dates/times
- upload/processing throughput counts
- failures and stuck queues with dataset IDs where useful
- API/storage/frontend/PostHog/Zulip/export status as checked
- backups status, including Borg archive freshness and backup-alert email review
- skipped or blocked surfaces
- confidence and one improvement for the next check

Use `green` only when all checked surfaces look normal. Use `yellow` for warnings,
stale jobs, missing signals, or server-side concerns. Use `red` for outages,
blocked processing, stale user-facing exports, critical storage pressure, or
multi-user impact.
