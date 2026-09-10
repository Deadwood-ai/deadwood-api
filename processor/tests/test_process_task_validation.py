from types import SimpleNamespace

import pytest

import processor.src.processor as processor_module
import processor.src.utils.queue_runtime as queue_runtime_module
from processor.src.processor import process_task
from shared.models import QueueTask, StatusEnum, TaskTypeEnum
from shared.settings import settings


# Keep these overrides local to this unit-test module. Processor conftest autouse
# fixtures normally prepare Supabase and SSH storage, but this guard is pure
# queue/status orchestration and should stay runnable without live services.
@pytest.fixture(scope='session', autouse=True)
def test_processor_user():
	yield None


@pytest.fixture(scope='session')
def auth_token():
	return 'processor-token'


@pytest.fixture(scope='session', autouse=True)
def cleanup_database():
	yield


@pytest.fixture(autouse=True)
def cleanup_storage():
	yield


@pytest.mark.unit
def test_process_task_rejects_downstream_without_geotiff(monkeypatch):
	"""Processor fails fast for manually inserted unsafe queue rows."""
	task = QueueTask(
		id=124,
		dataset_id=457,
		user_id='test-user',
		task_types=[TaskTypeEnum.thumbnail],
		priority=1,
		is_processing=False,
		current_position=1,
		estimated_time=0.0,
	)
	status_updates = []
	queue_updates = []
	deleted_task_ids = []

	class _DeleteQuery:
		def eq(self, field, value):
			assert field == 'id'
			deleted_task_ids.append(value)
			return self

		def execute(self):
			return None

	class _UpdateQuery:
		def __init__(self, payload):
			self.payload = payload

		def eq(self, field, value):
			assert field == 'id'
			queue_updates.append((value, self.payload))
			return self

		def execute(self):
			return None

	class _TableQuery:
		def update(self, payload):
			return _UpdateQuery(payload)

		def delete(self):
			return _DeleteQuery()

	class _FakeClient:
		def table(self, name):
			assert name == settings.queue_table
			return _TableQuery()

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

	def _record_status_update(*args, **kwargs):
		status_updates.append(kwargs)
		return SimpleNamespace(data=[])

	monkeypatch.setattr(processor_module, 'verify_token', lambda token: {'id': 'processor-user'})
	monkeypatch.setattr(processor_module, 'login', lambda username, password: 'delete-token')
	monkeypatch.setattr(queue_runtime_module, 'use_client', lambda token: _FakeClient())
	monkeypatch.setattr(processor_module, 'update_status', _record_status_update)
	monkeypatch.setattr(processor_module, 'create_processing_failure_issue', lambda **kwargs: None)
	monkeypatch.setattr(processor_module, '_notify_processing_result_safely', lambda *args: None)
	monkeypatch.setattr(processor_module.logger, 'info', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module.logger, 'error', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module.logger, 'warning', lambda *args, **kwargs: None)

	with pytest.raises(processor_module.ProcessingError) as exc_info:
		process_task(task, 'initial-token')

	assert 'require geotiff in the same processing request' in str(exc_info.value)
	assert queue_updates == []
	assert deleted_task_ids == [task.id]
	assert status_updates == [
		{
			'dataset_id': task.dataset_id,
			'current_status': StatusEnum.idle,
			'has_error': True,
			'error_message': str(exc_info.value),
			'error_stage': 'geotiff_dependency',
		}
	]
