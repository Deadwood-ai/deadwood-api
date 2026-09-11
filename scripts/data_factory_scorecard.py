#!/usr/bin/env python3
"""Compact DeadTrees data-factory scorecard.

This script reports the backend side of the DeadTrees data factory from
Postgres. Frontend pain and funnel signals still come from PostHog connector
queries; use --posthog-sql to print the compact query templates.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE_FILE = ROOT / '.local/operator/data-factory-scorecard.json'
DEFAULT_LATEST_FILE = ROOT / '.local/operator/data-factory-scorecard.md'


SCORECARD_SQL = r"""
with input as (
  select jsonb_build_object(
    'datasets_created_24h', count(*) filter (where created_at >= now() - interval '24 hours'),
    'datasets_created_7d', count(*) filter (where created_at >= now() - interval '7 days'),
    'unique_contributors_7d', count(distinct user_id) filter (where created_at >= now() - interval '7 days'),
    'total_datasets', count(*),
    'metadata_missing_acquisition_date', count(*) filter (where aquisition_year is null),
    'with_citation_doi', count(*) filter (where nullif(citation_doi, '') is not null),
    'archived', count(*) filter (where archived)
  ) as data
  from v2_datasets
), processing as (
  select jsonb_build_object(
    'complete_now', count(*) filter (
      where is_upload_done
        and is_cog_done
        and is_thumbnail_done
        and is_deadwood_done
        and is_forest_cover_done
    ),
    'has_error_now', count(*) filter (where has_error),
    'statuses_error_24h', count(*) filter (where updated_at >= now() - interval '24 hours' and has_error),
    'non_idle_now', count(*) filter (where current_status <> 'idle'),
    'latest_status_update', max(updated_at),
    'current_status_counts', coalesce(
      (
        select jsonb_object_agg(current_status::text, count)
        from (
          select current_status, count(*)::int as count
          from v2_statuses
          group by current_status
        ) s
      ),
      '{}'::jsonb
    )
  ) as data
  from v2_statuses
), queue as (
  select jsonb_build_object(
    'active', count(*) filter (where is_processing),
    'waiting', count(*) filter (where not is_processing),
    'oldest_active', min(created_at) filter (where is_processing),
    'active_items', coalesce(
      (
        select jsonb_agg(x)
        from (
          select id, dataset_id, priority, created_at, task_types
          from v2_queue
          where is_processing
          order by created_at
          limit 5
        ) x
      ),
      '[]'::jsonb
    ),
    'by_priority', coalesce(
      (
        select jsonb_agg(q order by is_processing desc, priority desc)
        from (
          select
            is_processing,
            priority,
            count(*)::int as count,
            min(created_at) as oldest,
            max(created_at) as newest
          from v2_queue
          group by is_processing, priority
        ) q
      ),
      '[]'::jsonb
    )
  ) as data
  from v2_queue
), trust as (
  select jsonb_build_object(
    'audits_reviewed_7d', (select count(*) from dataset_audit where reviewed_at >= now() - interval '7 days'),
    'audits_with_major_issue', (select count(*) from dataset_audit where has_major_issue),
    'open_flags', (select count(*) from dataset_flags where status is distinct from 'resolved'),
    'flags_created_7d', (select count(*) from dataset_flags where created_at >= now() - interval '7 days'),
    'reference_patches_total', (select count(*) from reference_patches),
    'reference_patches_validated_both', (
      select count(*)
      from reference_patches
      where deadwood_validated and forest_cover_validated
    ),
    'reference_patches_created_7d', (
      select count(*)
      from reference_patches
      where created_at >= now() - interval '7 days'
    )
  ) as data
), impact as (
  select jsonb_build_object(
    'published_release_versions', (
      select count(*)
      from prepackaged_dataset_versions
      where published_at is not null
    ),
    'release_versions_created_30d', (
      select count(*)
      from prepackaged_dataset_versions
      where created_at >= now() - interval '30 days'
    ),
    'prepackaged_signed_downloads_7d', (
      select count(*)
      from prepackaged_dataset_download_grants
      where created_at >= now() - interval '7 days'
        and extra->>'event' = 'prepackaged_signed_download_created'
    ),
    'prepackaged_signed_download_bytes_7d', (
      select coalesce(sum((extra->>'size_bytes')::bigint), 0)
      from prepackaged_dataset_download_grants
      where created_at >= now() - interval '7 days'
        and extra->>'event' = 'prepackaged_signed_download_created'
        and extra->>'size_bytes' ~ '^[0-9]+$'
    ),
    'datasets_with_freidata_doi', (
      select count(*)
      from v2_full_dataset_view
      where nullif(freidata_doi, '') is not null
    )
  ) as data
)
select jsonb_build_object(
  'checked_at', now(),
  'input', input.data,
  'processing', processing.data,
  'queue', queue.data,
  'trust', trust.data,
  'impact', impact.data
) as scorecard
from input, processing, queue, trust, impact;
""".strip()


POSTHOG_TOTALS_SQL = r"""
SELECT event, count() AS count
FROM events
WHERE timestamp >= now() - INTERVAL 24 HOUR
  AND event IN (
    '$pageview', '$exception', '$rageclick', '$dead_click',
    'dataset_opened', 'processing_result_viewed',
    'upload_started', 'upload_completed', 'upload_failed',
    'dataset_download_started', 'dataset_download_completed', 'dataset_download_failed'
  )
GROUP BY event
ORDER BY count DESC
LIMIT 50
""".strip()


POSTHOG_FRICTION_SQL = r"""
SELECT
  event,
  coalesce(properties['$pathname'], properties['$current_url'], 'unknown') AS route,
  count() AS count
FROM events
WHERE timestamp >= now() - INTERVAL 24 HOUR
  AND event IN ('$exception', '$rageclick', '$dead_click')
GROUP BY event, route
ORDER BY count DESC
LIMIT 12
""".strip()


def utc_now() -> str:
	return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def run_psql(database_url: str, timeout: int) -> dict[str, Any]:
	try:
		completed = subprocess.run(
			['psql', database_url, '-XAtq', '-v', 'ON_ERROR_STOP=1', '-c', SCORECARD_SQL],
			cwd=ROOT,
			text=True,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			timeout=timeout,
			check=False,
		)
	except FileNotFoundError:
		return {'ok': False, 'error': 'psql not found'}
	except subprocess.TimeoutExpired:
		return {'ok': False, 'error': f'psql timed out after {timeout}s'}

	if completed.returncode != 0:
		return {'ok': False, 'error': (completed.stderr or completed.stdout).strip().splitlines()[0][:240]}
	try:
		scorecard = json.loads(completed.stdout.strip())
	except json.JSONDecodeError:
		return {'ok': False, 'error': 'psql did not return JSON'}
	return {'ok': True, 'scorecard': scorecard}


def render_markdown(payload: dict[str, Any]) -> str:
	if not payload.get('ok'):
		return render_manual(payload)

	scorecard = payload['scorecard']
	input_data = scorecard['input']
	processing = scorecard['processing']
	queue = scorecard['queue']
	trust = scorecard['trust']
	impact = scorecard['impact']
	verdict, constraint, risks = classify(scorecard)

	lines = [
		f"# DeadTrees Data Factory Scorecard ({scorecard.get('checked_at') or utc_now()})",
		'',
		f"- verdict: `{verdict}`",
		f"- current constraint: `{constraint}`",
		'',
		'## Input',
		f"- datasets created: 24h `{input_data['datasets_created_24h']}`, 7d `{input_data['datasets_created_7d']}`",
		f"- unique contributors 7d: `{input_data['unique_contributors_7d']}`",
		f"- total datasets: `{input_data['total_datasets']}`; archived `{input_data['archived']}`",
		f"- citation DOI coverage: `{input_data['with_citation_doi']}` datasets",
		f"- missing acquisition date: `{input_data['metadata_missing_acquisition_date']}`",
		'',
		'## Processing',
		f"- complete now: `{processing['complete_now']}`",
		f"- non-idle now: `{processing['non_idle_now']}`; has_error now `{processing['has_error_now']}`",
		f"- status errors 24h: `{processing['statuses_error_24h']}`",
		f"- active queue: `{queue['active']}`; waiting `{queue['waiting']}`",
	]
	for item in queue.get('active_items', [])[:3]:
		lines.append(
			f"- active item: queue `{item['id']}`, dataset `{item['dataset_id']}`, "
			f"priority `{item['priority']}`, since `{item['created_at']}`"
		)
	lines.extend(
		[
			'',
			'## Trust',
			f"- audits reviewed 7d: `{trust['audits_reviewed_7d']}`",
			f"- audits with major issue: `{trust['audits_with_major_issue']}`",
			f"- open flags: `{trust['open_flags']}`; flags created 7d `{trust['flags_created_7d']}`",
			f"- reference patches: `{trust['reference_patches_validated_both']}` validated both / `{trust['reference_patches_total']}` total",
			f"- reference patches created 7d: `{trust['reference_patches_created_7d']}`",
			'',
			'## Impact',
			f"- published release versions: `{impact['published_release_versions']}`",
			f"- release versions created 30d: `{impact['release_versions_created_30d']}`",
			f"- prepackaged signed downloads 7d: `{impact['prepackaged_signed_downloads_7d']}`; bytes `{impact['prepackaged_signed_download_bytes_7d']}`",
			f"- datasets with FreiDATA DOI: `{impact['datasets_with_freidata_doi']}`",
			'',
			'## Risks',
			*bullet_lines(risks or ['none']),
			'',
			'## PostHog Add-On',
			'- Run `python3 scripts/data_factory_scorecard.py --posthog-sql` and execute the two SQL snippets through the PostHog MCP after schema discovery.',
			'- Use recordings only from a concrete `$dead_click`, `$rageclick`, or `$exception` route cluster.',
		]
	)
	return '\n'.join(lines) + '\n'


def classify(scorecard: dict[str, Any]) -> tuple[str, str, list[str]]:
	risks: list[str] = []
	processing = scorecard['processing']
	queue = scorecard['queue']
	trust = scorecard['trust']
	impact = scorecard['impact']

	if int(processing['statuses_error_24h']) > 0:
		risks.append(f"{processing['statuses_error_24h']} status error(s) in 24h")
	if int(queue['active']) > 0:
		risks.append(f"{queue['active']} active processing queue item(s)")
	if int(processing['has_error_now']) > 0:
		risks.append(f"{processing['has_error_now']} datasets currently have errors")
	if int(trust['open_flags']) > 0:
		risks.append(f"{trust['open_flags']} open dataset flags")
	if int(impact['prepackaged_signed_downloads_7d']) == 0:
		risks.append('no prepackaged signed downloads in 7d')

	constraint = 'processing reliability'
	if int(processing['statuses_error_24h']) == 0 and int(queue['active']) <= 1:
		constraint = 'trust/reference throughput'
	if int(trust['audits_reviewed_7d']) > 0 and int(trust['reference_patches_created_7d']) == 0:
		constraint = 'reference-patch creation'
	if int(impact['prepackaged_signed_downloads_7d']) == 0:
		constraint = 'reuse/impact capture'

	verdict = 'green'
	if risks:
		verdict = 'yellow'
	if int(processing['statuses_error_24h']) > 0:
		verdict = 'red'
	return verdict, constraint, risks[:5]


def render_manual(payload: dict[str, Any]) -> str:
	lines = [
		f"# DeadTrees Data Factory Scorecard ({utc_now()})",
		'',
		'- verdict: `manual`',
		f"- DB scorecard skipped: {payload.get('error') or payload.get('skipped')}",
		'',
		'For routine production reads, follow docs/playbooks/analyst-database-access.md.',
		'Print the SQL, then execute it inside the verified analyst read-only transaction:',
		'',
		'```bash',
		'python3 scripts/data_factory_scorecard.py --print-sql',
		'```',
	]
	return '\n'.join(lines) + '\n'


def bullet_lines(values: list[str]) -> list[str]:
	return [f'- {value}' for value in values]


def write_state(payload: dict[str, Any], state_file: Path, latest_file: Path) -> None:
	state_file.parent.mkdir(parents=True, exist_ok=True)
	state_file.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n')
	latest_file.write_text(render_markdown(payload))


def parse_args(argv: list[str]) -> argparse.Namespace:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument('--format', choices=['markdown', 'json'], default='markdown')
	parser.add_argument('--timeout', type=int, default=20)
	parser.add_argument('--write-state', action='store_true')
	parser.add_argument('--state-file', type=Path, default=DEFAULT_STATE_FILE)
	parser.add_argument('--latest-file', type=Path, default=DEFAULT_LATEST_FILE)
	parser.add_argument('--print-sql', action='store_true')
	parser.add_argument('--posthog-sql', action='store_true')
	return parser.parse_args(argv)


def main(argv: list[str]) -> int:
	args = parse_args(argv)
	if args.print_sql:
		print(SCORECARD_SQL)
		return 0
	if args.posthog_sql:
		print('-- PostHog event totals')
		print(POSTHOG_TOTALS_SQL)
		print('\n-- PostHog friction routes')
		print(POSTHOG_FRICTION_SQL)
		return 0

	database_url = os.environ.get('DEADTREES_OPERATOR_DATABASE_URL')
	payload = {'ok': None, 'skipped': 'set DEADTREES_OPERATOR_DATABASE_URL for backend scorecard'}
	if database_url:
		payload = run_psql(database_url, args.timeout)

	if args.write_state:
		write_state(payload, args.state_file, args.latest_file)
	if args.format == 'json':
		print(json.dumps(payload, indent=2, sort_keys=True))
	else:
		print(render_markdown(payload), end='')
	return 0 if payload.get('ok') is not False else 2


if __name__ == '__main__':
	raise SystemExit(main(sys.argv[1:]))
