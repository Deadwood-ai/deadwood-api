from datetime import datetime

import pytest

from shared.models import StatusEnum
from shared.status import update_status


def test_update_status_refreshes_updated_at(monkeypatch):
	updates = []
	inserts = []

	class _ExecuteResult:
		data = [{'id': 1}]

	class _EqQuery:
		def __init__(self, payload=None):
			self.payload = payload

		def eq(self, field, value):
			assert field == 'dataset_id'
			assert value == 123
			return self

		def execute(self):
			return _ExecuteResult()

	class _Table:
		def select(self, fields):
			assert fields == 'id'
			return _EqQuery()

		def update(self, payload):
			updates.append(payload)
			return _EqQuery(payload)

		def insert(self, payload):
			inserts.append(payload)
			return _EqQuery(payload)

	class _Client:
		def table(self, name):
			assert name == 'v2_statuses'
			return _Table()

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

	monkeypatch.setattr('shared.status.use_client', lambda token: _Client())

	update_status('token', dataset_id=123, current_status=StatusEnum.ortho_processing)

	assert inserts == []
	assert len(updates) == 1
	assert updates[0]['current_status'] == StatusEnum.ortho_processing
	updated_at = datetime.fromisoformat(updates[0]['updated_at'])
	assert updated_at.tzinfo is not None


def test_update_status_sets_updated_at_when_creating_status(monkeypatch):
	inserts = []

	class _EmptyResult:
		data = []

	class _EqQuery:
		def eq(self, field, value):
			assert field == 'dataset_id'
			assert value == 123
			return self

		def execute(self):
			return _EmptyResult()

	class _InsertQuery:
		def execute(self):
			return None

	class _Table:
		def select(self, fields):
			assert fields == 'id'
			return _EqQuery()

		def insert(self, payload):
			inserts.append(payload)
			return _InsertQuery()

	class _Client:
		def table(self, name):
			assert name == 'v2_statuses'
			return _Table()

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

	monkeypatch.setattr('shared.status.use_client', lambda token: _Client())

	update_status('token', dataset_id=123, is_upload_done=True)

	assert len(inserts) == 1
	assert inserts[0]['dataset_id'] == 123
	assert inserts[0]['is_upload_done'] is True
	updated_at = datetime.fromisoformat(inserts[0]['updated_at'])
	assert updated_at.tzinfo is not None


def _make_recording_client(updates):
	"""Build a fake supabase client that records status updates."""

	class _ExecuteResult:
		data = [{'id': 1}]

	class _EqQuery:
		def eq(self, field, value):
			return self

		def execute(self):
			return _ExecuteResult()

	class _Table:
		def select(self, fields):
			return _EqQuery()

		def update(self, payload):
			updates.append(payload)
			return _EqQuery()

	class _Client:
		def table(self, name):
			return _Table()

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

	return _Client()


def test_update_status_retries_on_transient_error(monkeypatch):
	"""A transient connection drop should be retried, not lost.

	This is the regression guard for the ghost-status bug: previously a single
	'Server disconnected' while writing the status left the dataset stuck with
	has_error=false and current_status != idle, which the requeue API refuses.
	"""
	updates = []
	calls = {'n': 0}
	monkeypatch.setattr('shared.retry.time.sleep', lambda d: None)

	def fake_use_client(token):
		calls['n'] += 1
		if calls['n'] < 3:
			raise Exception('Server disconnected without sending a response.')
		return _make_recording_client(updates)

	monkeypatch.setattr('shared.status.use_client', fake_use_client)

	update_status('token', dataset_id=123, has_error=True, error_message='boom')

	assert calls['n'] == 3
	assert len(updates) == 1
	assert updates[0]['has_error'] is True


def test_update_status_does_not_retry_non_transient_error(monkeypatch):
	calls = {'n': 0}
	monkeypatch.setattr('shared.retry.time.sleep', lambda d: None)

	def fake_use_client(token):
		calls['n'] += 1
		raise ValueError('programming bug')

	monkeypatch.setattr('shared.status.use_client', fake_use_client)

	with pytest.raises(ValueError, match='programming bug'):
		update_status('token', dataset_id=123, is_cog_done=True)

	assert calls['n'] == 1


def test_failure_stage_survives_progress_updates_and_clears_on_retry(monkeypatch):
	updates = []
	monkeypatch.setattr('shared.status.use_client', lambda token: _make_recording_client(updates))
	update_status('token', 123, has_error=True, error_stage='cog_processing')
	update_status('token', 123, is_thumbnail_done=True)
	update_status('token', 123, has_error=False)
	assert updates[0]['error_stage'] == 'cog_processing'
	assert 'error_stage' not in updates[1]
	assert updates[2]['error_stage'] is None


def test_unknown_failure_does_not_reuse_an_older_stage(monkeypatch):
	updates = []
	monkeypatch.setattr('shared.status.use_client', lambda token: _make_recording_client(updates))
	update_status('token', 123, has_error=True)
	assert updates[0]['error_stage'] is None
