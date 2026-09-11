#!/usr/bin/env python3
"""Compact DeadTrees operator status snapshot.

This script is intentionally dependency-free and safe to track. Production-only
checks are opt-in through local environment variables so secrets stay out of Git.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_API_URL = 'https://data2.deadtrees.earth/api/v1/openapi.json'
DEFAULT_STATE_FILE = ROOT / '.local/operator/operator-state.json'
DEFAULT_LATEST_FILE = ROOT / '.local/operator/operator-latest.md'
DEFAULT_BACKUP_MAX_AGE_HOURS = 36
ARCHIVE_TIMESTAMP_RE = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})')


def utc_now() -> str:
	return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def run_command(args: list[str], timeout: int = 10, cwd: Path = ROOT) -> dict[str, Any]:
	start = time.monotonic()
	try:
		completed = subprocess.run(
			args,
			cwd=cwd,
			text=True,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			timeout=timeout,
			check=False,
		)
	except FileNotFoundError:
		return {'ok': False, 'error': f'{args[0]} not found', 'duration_ms': elapsed_ms(start)}
	except subprocess.TimeoutExpired:
		return {'ok': False, 'error': f'timed out after {timeout}s', 'duration_ms': elapsed_ms(start)}

	return {
		'ok': completed.returncode == 0,
		'returncode': completed.returncode,
		'stdout': completed.stdout.strip(),
		'stderr': completed.stderr.strip(),
		'duration_ms': elapsed_ms(start),
	}


def elapsed_ms(start: float) -> int:
	return int((time.monotonic() - start) * 1000)


def compact_error(result: dict[str, Any]) -> str:
	return str(result.get('stderr') or result.get('error') or result.get('stdout') or 'unknown error').splitlines()[0][:240]


def git_status() -> dict[str, Any]:
	status = run_command(['git', 'status', '--porcelain=v1', '--branch'], timeout=5)
	if not status['ok']:
		return {'ok': False, 'error': compact_error(status)}

	lines = status['stdout'].splitlines()
	branch_line = lines[0] if lines else ''
	dirty_files = [line[3:] for line in lines[1:] if len(line) >= 4]
	divergence = {'ahead': 0, 'behind': 0}
	if '[ahead ' in branch_line:
		divergence['ahead'] = int(branch_line.split('[ahead ', 1)[1].split(']', 1)[0].split(',', 1)[0])
	if 'behind ' in branch_line:
		divergence['behind'] = int(branch_line.split('behind ', 1)[1].split(']', 1)[0].split(',', 1)[0])

	head = run_command(['git', 'rev-parse', '--short', 'HEAD'], timeout=5)
	upstream = run_command(['git', 'rev-parse', '--abbrev-ref', '--symbolic-full-name', '@{u}'], timeout=5)
	ahead_commits: list[str] = []
	behind_commits: list[str] = []
	if upstream['ok']:
		if divergence['ahead']:
			ahead = run_command(['git', 'log', '--oneline', '--max-count=3', f'{upstream["stdout"]}..HEAD'], timeout=5)
			ahead_commits = ahead['stdout'].splitlines() if ahead['ok'] else []
		if divergence['behind']:
			behind = run_command(['git', 'log', '--oneline', '--max-count=3', f'HEAD..{upstream["stdout"]}'], timeout=5)
			behind_commits = behind['stdout'].splitlines() if behind['ok'] else []
	return {
		'ok': True,
		'branch': branch_line.removeprefix('## '),
		'head': head['stdout'] if head['ok'] else None,
		'upstream': upstream['stdout'] if upstream['ok'] else None,
		'dirty_count': len(dirty_files),
		'dirty_files': dirty_files[:8],
		'divergence': divergence,
		'ahead_commits': ahead_commits,
		'behind_commits': behind_commits,
	}


def api_health(url: str, timeout: int) -> dict[str, Any]:
	start = time.monotonic()
	try:
		request = urllib.request.Request(url, headers={'User-Agent': 'deadtrees-operator-status/1'})
		with urllib.request.urlopen(request, timeout=timeout) as response:
			body = response.read(160_000)
			payload = json.loads(body.decode('utf-8'))
	except urllib.error.HTTPError as exc:
		return {'ok': False, 'url': url, 'status': exc.code, 'error': str(exc), 'duration_ms': elapsed_ms(start)}
	except Exception as exc:  # noqa: BLE001 - compact operator probe should not crash on network issues.
		return {'ok': False, 'url': url, 'error': str(exc)[:240], 'duration_ms': elapsed_ms(start)}

	return {
		'ok': True,
		'url': url,
		'status': response.status,
		'openapi': payload.get('openapi'),
		'title': payload.get('info', {}).get('title'),
		'version': payload.get('info', {}).get('version'),
		'duration_ms': elapsed_ms(start),
	}


def database_summary(window_hours: int, timeout: int) -> dict[str, Any]:
	database_url = os.environ.get('DEADTREES_OPERATOR_DATABASE_URL')
	if not database_url:
		return {'ok': None, 'skipped': 'set DEADTREES_OPERATOR_DATABASE_URL for compact read-only DB summary'}

	sql = f"""
with status_summary as (
	select
		count(*) filter (where created_at >= now() - interval '{window_hours} hours') as datasets_created,
		count(*) filter (where updated_at >= now() - interval '{window_hours} hours' and has_error) as statuses_error,
		count(*) filter (where current_status <> 'idle') as non_idle_now,
		max(updated_at) as latest_status_update
	from v2_statuses
),
log_summary as (
	select coalesce(jsonb_object_agg(level, count), '{{}}'::jsonb) as counts
	from (
		select level, count(*)::int as count
		from v2_logs
		where created_at >= now() - interval '{window_hours} hours'
		group by level
	) levels
),
queue_summary as (
	select
		count(*) filter (where is_processing)::int as active,
		min(created_at) filter (where is_processing) as oldest_active,
		count(*)::int as total
	from v2_queue
)
select jsonb_build_object(
	'datasets_created_window', status_summary.datasets_created,
	'statuses_error_window', status_summary.statuses_error,
	'non_idle_now', status_summary.non_idle_now,
	'latest_status_update', status_summary.latest_status_update,
	'log_counts_window', log_summary.counts,
	'queue_active', queue_summary.active,
	'queue_total', queue_summary.total,
	'queue_oldest_active', queue_summary.oldest_active
)
from status_summary, log_summary, queue_summary;
"""
	result = run_command(
		['psql', database_url, '-XAtq', '-v', 'ON_ERROR_STOP=1', '-c', sql],
		timeout=timeout,
	)
	if not result['ok']:
		return {'ok': False, 'error': compact_error(result)}

	try:
		payload = json.loads(result['stdout'])
	except json.JSONDecodeError:
		return {'ok': False, 'error': 'psql did not return JSON'}

	payload['ok'] = True
	payload['window_hours'] = window_hours
	return payload


def ssh_probe(env_name: str, command: str, timeout: int) -> dict[str, Any]:
	host = os.environ.get(env_name)
	if not host:
		return {'ok': None, 'skipped': f'set {env_name} for host probe'}

	result = run_command(
		['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', host, command],
		timeout=timeout,
	)
	if not result['ok']:
		return {'ok': False, 'host_env': env_name, 'error': compact_error(result)}

	lines = [line.strip() for line in result['stdout'].splitlines() if line.strip()]
	archives = parse_archive_lines(lines)
	return {
		'ok': True,
		'host_env': env_name,
		'lines': lines[:12],
		'disks': parse_disk_lines(lines),
		'archives': archives,
		'archive_ages_hours': parse_archive_ages_hours(archives),
		'duration_ms': result['duration_ms'],
	}


def parse_disk_lines(lines: list[str]) -> dict[str, int]:
	disks: dict[str, int] = {}
	for line in lines:
		if not line.startswith('disk=') or ':' not in line:
			continue
		path, percent = line.removeprefix('disk=').rsplit(':', 1)
		try:
			disks[path] = int(percent.rstrip('%'))
		except ValueError:
			continue
	return disks


def parse_archive_lines(lines: list[str]) -> dict[str, str]:
	archives: dict[str, str] = {}
	for line in lines:
		if not line.startswith('archive=') or ':' not in line:
			continue
		name, archive = line.removeprefix('archive=').split(':', 1)
		if name and archive:
			archives[name] = archive
	return archives


def parse_archive_ages_hours(archives: dict[str, str]) -> dict[str, float]:
	now = datetime.now(timezone.utc)
	ages: dict[str, float] = {}
	for name, archive in archives.items():
		match = ARCHIVE_TIMESTAMP_RE.search(archive)
		if not match:
			continue
		try:
			archive_time = datetime.fromisoformat(match.group(1)).replace(tzinfo=timezone.utc)
		except ValueError:
			continue
		ages[name] = round((now - archive_time).total_seconds() / 3600, 1)
	return ages


def backup_command() -> str:
	configured = os.environ.get('DEADTREES_OPERATOR_BACKUP_COMMAND')
	if configured:
		return configured

	backup_path = os.environ.get('DEADTREES_OPERATOR_BACKUP_PATH')
	if backup_path:
		quoted_path = shlex.quote(backup_path)
		return (
			f"find {quoted_path} -type f -printf '%T@ %TY-%Tm-%TdT%TH:%TM %p\\n' 2>/dev/null "
			"| sort -nr | head -n 5"
		)

	return (
		"set -e -o pipefail; "
		"borgmatic=/home/remote-backup/.local/bin/borgmatic; "
		"config_dir=/home/remote-backup/.config/borgmatic; "
		"if [ ! -x \"$borgmatic\" ]; then echo \"borgmatic not executable: $borgmatic\" >&2; exit 1; fi; "
		"for spec in database_dump:database_dump_direct.yaml storage:storage.yaml; do "
		"name=${spec%%:*}; "
		"config=$config_dir/${spec#*:}; "
		"latest=$(\"$borgmatic\" --config \"$config\" list "
		"| awk -v backup=\"$name\" "
		"'/^[[:alnum:]_.-]+-[0-9]{4}-[0-9]{2}-[0-9]{2}T/ { "
		"if (backup != \"database_dump\" || $1 !~ /[.]checkpoint([.][0-9]+)?$/) latest=$1 "
		"} END { print latest }'); "
		"test -n \"$latest\"; "
		"printf 'archive=%s:%s\\n' \"$name\" \"$latest\"; "
		"done"
	)


def host_summaries(timeout: int) -> dict[str, Any]:
	df_command = (
		"printf 'host=%s\\n' \"$(hostname)\"; "
		"df -P / /data 2>/dev/null | awk 'NR>1 {print \"disk=\" $6 \":\" $5}'"
	)
	return {
		'processing': ssh_probe('DEADTREES_OPERATOR_PROCESSING_HOST', df_command, timeout),
		'storage': ssh_probe('DEADTREES_OPERATOR_STORAGE_HOST', df_command, timeout),
		'backups': ssh_probe('DEADTREES_OPERATOR_BACKUP_HOST', backup_command(), timeout),
	}


def connector_placeholders() -> dict[str, Any]:
	return {
		'posthog': {
			'ok': None,
			'manual_connector_check': 'event counts, exceptions, rage/dead clicks, drop-off deltas; recordings only after a concrete pain signal',
		},
		'linear': {'ok': None, 'manual_connector_check': 'active blockers, stale triage, repeated fingerprints, drift'},
		'gmail': {'ok': None, 'manual_connector_check': 'urgent project delta only; inspect likely actionable threads'},
		'zulip': {'ok': None, 'manual_connector_check': 'monitored unread delta; history pass only after downtime or anomaly'},
	}


def load_previous(path: Path) -> dict[str, Any] | None:
	try:
		return json.loads(path.read_text())
	except FileNotFoundError:
		return None
	except json.JSONDecodeError:
		return {'error': 'previous state is not valid JSON'}


def classify(snapshot: dict[str, Any]) -> tuple[str, list[str], list[str]]:
	risks: list[str] = []
	skipped: list[str] = []

	git = snapshot['repo']
	if git.get('dirty_count'):
		risks.append(f"repo has {git['dirty_count']} dirty file(s)")
	if git.get('divergence', {}).get('behind'):
		risks.append(f"repo is behind origin by {git['divergence']['behind']} commit(s)")

	api = snapshot['platform']['api']
	if api.get('ok') is False:
		risks.append(f"API smoke failed: {api.get('error') or api.get('status')}")

	db = snapshot['platform']['database']
	if db.get('ok') is False:
		risks.append(f"database summary failed: {db.get('error')}")
	elif db.get('ok') is None:
		skipped.append('database')
	else:
		if int(db.get('statuses_error_window') or 0) > 0:
			risks.append(f"{db['statuses_error_window']} status error(s) in window")
		if int(db.get('queue_active') or 0) > 0:
			risks.append(f"{db['queue_active']} active queue item(s)")

	for key, value in snapshot['platform']['hosts'].items():
		if value.get('ok') is False:
			risks.append(f"{key} host probe failed")
		elif value.get('ok') is None:
			skipped.append(key)
		for path, percent in value.get('disks', {}).items():
			if percent >= 80:
				risks.append(f'{key} disk {path} is {percent}% full')
		if key == 'backups' and value.get('archive_ages_hours'):
			max_age_hours = int(os.environ.get('DEADTREES_OPERATOR_BACKUP_MAX_AGE_HOURS', DEFAULT_BACKUP_MAX_AGE_HOURS))
			for archive_name, age_hours in value['archive_ages_hours'].items():
				if age_hours > max_age_hours:
					risks.append(f'{archive_name} backup archive is {age_hours}h old')

	for key, value in snapshot['connectors'].items():
		if value.get('ok') is None:
			skipped.append(key)

	if any('failed' in risk for risk in risks):
		return 'red', risks[:5], skipped
	if risks:
		return 'yellow', risks[:5], skipped
	return 'green', risks, skipped


def changed_since_last(previous: dict[str, Any] | None, snapshot: dict[str, Any]) -> list[str]:
	if not previous or 'snapshot' not in previous:
		return ['no previous state']

	old = previous['snapshot']
	changes: list[str] = []
	for path, label in [
		(('verdict',), 'verdict'),
		(('repo', 'head'), 'git HEAD'),
		(('platform', 'database', 'statuses_error_window'), 'DB status errors'),
		(('platform', 'database', 'queue_active'), 'active queue count'),
	]:
		old_value = nested_get(old, path)
		new_value = nested_get(snapshot, path)
		if old_value != new_value:
			changes.append(f'{label}: {old_value} -> {new_value}')
	return changes[:8] or ['no tracked changes']


def nested_get(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
	current: Any = payload
	for key in path:
		if not isinstance(current, dict):
			return None
		current = current.get(key)
	return current


def build_snapshot(args: argparse.Namespace) -> dict[str, Any]:
	previous = load_previous(args.state_file)
	snapshot: dict[str, Any] = {
		'checked_at': utc_now(),
		'window_hours': args.window_hours,
		'repo': git_status(),
		'platform': {
			'api': {'ok': None, 'skipped': '--skip-network'} if args.skip_network else api_health(args.api_url, args.timeout),
			'database': database_summary(args.window_hours, args.db_timeout),
			'hosts': host_summaries(args.ssh_timeout),
		},
		'connectors': connector_placeholders(),
	}
	verdict, risks, skipped = classify(snapshot)
	snapshot['verdict'] = verdict
	snapshot['top_risks'] = risks
	snapshot['skipped_surfaces'] = skipped
	snapshot['changed_since_last'] = changed_since_last(previous, snapshot)
	snapshot['next_checks'] = next_checks(snapshot)
	return snapshot


def next_checks(snapshot: dict[str, Any]) -> list[str]:
	checks: list[str] = []
	repo = snapshot['repo']
	if repo.get('divergence', {}).get('behind'):
		checks.append('inspect upstream commits, then pull only if the tree is clean')
	if snapshot['platform']['database'].get('ok') is None:
		checks.append('run DB aggregates via docs/playbooks/analyst-database-access.md; existing monitor probes use DEADTREES_OPERATOR_DATABASE_URL')
	if snapshot['platform']['hosts']['backups'].get('ok') is None:
		checks.append('configure backup host/path or command for freshness probe')
	checks.append('run PostHog/Linear/Gmail/Zulip connector deltas')
	return checks[:4]


def render_markdown(snapshot: dict[str, Any]) -> str:
	db = snapshot['platform']['database']
	api = snapshot['platform']['api']
	lines = [
		f"# DeadTrees Operator Status ({snapshot['checked_at']})",
		'',
		f"- verdict: `{snapshot['verdict']}`",
		f"- window: `{snapshot['window_hours']}h`",
		f"- repo: `{snapshot['repo'].get('branch')}` head `{snapshot['repo'].get('head')}` dirty `{snapshot['repo'].get('dirty_count')}`",
			f"- API: `{status_word(api)}` version `{api.get('version')}`",
			f"- DB: `{status_word(db)}` errors `{db.get('statuses_error_window')}` non-idle `{db.get('non_idle_now')}` active-queue `{db.get('queue_active')}`",
		]
	if snapshot['repo'].get('behind_commits'):
		lines.extend(['', '## Upstream Commits', *bullet_lines(snapshot['repo']['behind_commits'])])
	if snapshot['repo'].get('ahead_commits'):
		lines.extend(['', '## Local Commits', *bullet_lines(snapshot['repo']['ahead_commits'])])
	if snapshot['repo'].get('dirty_files'):
		lines.extend(['', '## Dirty Files', *bullet_lines(snapshot['repo']['dirty_files'])])
	lines.extend(
		[
			'',
			'## Changed Since Last',
			*bullet_lines(snapshot['changed_since_last']),
			'',
			'## Top Risks',
			*bullet_lines(snapshot['top_risks'] or ['none']),
			'',
			'## Host And Backup Probes',
		]
	)
	for key, value in snapshot['platform']['hosts'].items():
		parts: list[str] = []
		if value.get('disks'):
			parts.append('disks ' + ', '.join(f'{path}={percent}%' for path, percent in value['disks'].items()))
		if value.get('archives'):
			archive_ages = value.get('archive_ages_hours', {})
			parts.append(
				'archives '
				+ ', '.join(
					f'{name}={archive}'
					+ (f' age={archive_ages[name]}h' if name in archive_ages else '')
					for name, archive in value['archives'].items()
				)
			)
		summary = '; '.join(parts)
		if not summary:
			summary = '; '.join(value.get('lines', [])[:4]) if value.get('lines') else value.get('skipped') or value.get('error')
		lines.append(f"- {key}: `{status_word(value)}` {summary or ''}")
	lines.extend(
		[
			'',
			'## Connector Surfaces',
		]
	)
	for key, value in snapshot['connectors'].items():
		lines.append(f"- {key}: `{status_word(value)}` {value.get('manual_connector_check')}")
	lines.extend(
		[
				'',
				'## Skipped Or Manual',
				*bullet_lines(snapshot['skipped_surfaces'] or ['none']),
				'',
				'## Next Checks',
				*bullet_lines(snapshot['next_checks']),
			]
		)
	if snapshot.get('state_write_error'):
		lines.extend(
			[
				'',
				'## State Write',
				f"- failed: {snapshot['state_write_error']}",
			]
		)
	return '\n'.join(lines) + '\n'


def status_word(payload: dict[str, Any]) -> str:
	if payload.get('ok') is True:
		return 'ok'
	if payload.get('ok') is False:
		return 'failed'
	return 'manual'


def bullet_lines(values: list[str]) -> list[str]:
	return [f"- {value}" for value in values]


def write_state(snapshot: dict[str, Any], state_file: Path, latest_file: Path) -> None:
	state_file.parent.mkdir(parents=True, exist_ok=True)
	state_file.write_text(json.dumps({'snapshot': snapshot}, indent=2, sort_keys=True) + '\n')
	latest_file.write_text(render_markdown(snapshot))


def parse_args(argv: list[str]) -> argparse.Namespace:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument('--format', choices=['markdown', 'json'], default='markdown')
	parser.add_argument('--window-hours', type=int, default=24)
	parser.add_argument('--api-url', default=DEFAULT_API_URL)
	parser.add_argument('--timeout', type=int, default=8)
	parser.add_argument('--db-timeout', type=int, default=12)
	parser.add_argument('--ssh-timeout', type=int, default=10)
	parser.add_argument('--skip-network', action='store_true')
	parser.add_argument('--write-state', action='store_true')
	parser.add_argument('--state-file', type=Path, default=DEFAULT_STATE_FILE)
	parser.add_argument('--latest-file', type=Path, default=DEFAULT_LATEST_FILE)
	return parser.parse_args(argv)


def main(argv: list[str]) -> int:
	args = parse_args(argv)
	snapshot = build_snapshot(args)
	if args.write_state:
		try:
			write_state(snapshot, args.state_file, args.latest_file)
		except OSError as exc:
			snapshot['state_write_error'] = str(exc)
	if args.format == 'json':
		print(json.dumps(snapshot, indent=2, sort_keys=True))
	else:
		print(render_markdown(snapshot), end='')
	return 0 if snapshot['verdict'] != 'red' else 2


if __name__ == '__main__':
	raise SystemExit(main(sys.argv[1:]))
