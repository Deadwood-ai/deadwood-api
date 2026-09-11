from types import SimpleNamespace
from time import time

import httpx
import pytest

import processor.src.processor as processor_module
import processor.src.utils.queue_runtime as queue_runtime_module
from processor.src import processing_notifications
from processor.src.processor import background_process, process_task
from shared.db import use_service_client
from shared.models import QueueTask, StatusEnum, TaskTypeEnum
from shared.notifications.processing import ProcessingNotificationBatch, ProcessingNotificationType
from shared.settings import settings


def _task() -> QueueTask:
	return QueueTask(
		id=42,
		dataset_id=84,
		user_id='requester',
		task_types=[TaskTypeEnum.metadata],
		priority=1,
		is_processing=True,
		claimed_by='worker-a',
		current_position=1,
	)


@pytest.mark.unit
def test_recording_failure_propagates_before_dispatch(monkeypatch):
	dispatch_calls = []

	def fail_record(*args):
		raise RuntimeError('outbox unavailable')

	monkeypatch.setattr(processing_notifications, 'record_processing_result', fail_record)
	monkeypatch.setattr(
		processing_notifications,
		'dispatch_processing_result',
		lambda batch: dispatch_calls.append(batch),
	)

	with pytest.raises(RuntimeError, match='outbox unavailable'):
		processing_notifications.notify_processing_result_safely(
			_task(), ProcessingNotificationType.completed, 'token'
		)

	assert dispatch_calls == []


@pytest.mark.unit
def test_dispatch_failure_returns_durable_events(monkeypatch):
	events = [{'id': 'event-id', 'status': 'pending'}]
	batch = SimpleNamespace(events=events)

	monkeypatch.setattr(processing_notifications, 'record_processing_result', lambda *args: batch)
	monkeypatch.setattr(
		processing_notifications,
		'dispatch_processing_result',
		lambda current_batch: (_ for _ in ()).throw(RuntimeError('provider unavailable')),
	)
	monkeypatch.setattr(processing_notifications.logger, 'info', lambda *args, **kwargs: None)
	monkeypatch.setattr(processing_notifications.logger, 'warning', lambda *args, **kwargs: None)

	result = processing_notifications.notify_processing_result_safely(
		_task(), ProcessingNotificationType.completed, 'token'
	)

	assert result == events


@pytest.mark.unit
def test_partial_success_without_notification_is_not_an_error(monkeypatch):
	batch = ProcessingNotificationBatch(events=[], file_name='forest.tif')
	monkeypatch.setattr(processing_notifications, 'record_processing_result', lambda *args: batch)

	result = processing_notifications.notify_processing_result_safely(
		_task(), ProcessingNotificationType.completed, 'token'
	)

	assert result == []


@pytest.mark.unit
def test_reconciliation_is_bounded_to_one_event(monkeypatch):
	limits = []
	monkeypatch.setattr(
		processing_notifications,
		'reconcile_processing_notifications',
		lambda limit: limits.append(limit) or [],
	)

	processing_notifications.reconcile_processing_notifications_safely()

	assert limits == [1]


@pytest.mark.integration
def test_processor_failure_notification_reaches_mailpit(monkeypatch, test_processor_user):
	assert settings.DEV_MODE is True
	mailpit_messages_url = f'{settings.MAILPIT_API_URL}/api/v1/messages'
	assert httpx.delete(mailpit_messages_url, timeout=5).status_code == 200

	dataset_id = None
	try:
		with use_service_client() as client:
			dataset = client.table(settings.datasets_table).insert({
				'file_name': 'processor-mailpit.tif',
				'user_id': test_processor_user,
				'license': 'CC BY',
				'platform': 'drone',
				'authors': ['Processor test'],
				'data_access': 'public',
				'aquisition_year': 2026,
			}).execute().data[0]
			dataset_id = dataset['id']
			client.table(settings.statuses_table).insert({
				'dataset_id': dataset_id,
				'current_status': 'idle',
				'has_error': True,
				'error_message': 'test failure',
			}).execute()

		monkeypatch.setattr(settings, 'PROCESSING_EMAIL_NOTIFICATIONS_ENABLED', True)
		task = QueueTask(
			id=int(time() * 1000),
			dataset_id=dataset_id,
			user_id=test_processor_user,
			task_types=[TaskTypeEnum.metadata],
			priority=1,
			is_processing=True,
			current_position=1,
		)

		result = processing_notifications.notify_processing_result_safely(
			task, ProcessingNotificationType.failed, 'token'
		)

		assert result[0]['status'] == 'sent'
		messages = httpx.get(mailpit_messages_url, params={'limit': 10}, timeout=5).json()['messages']
		assert any(message['Subject'] == f'Dataset {dataset_id} - Processing Failed' for message in messages)
	finally:
		if dataset_id is not None:
			with use_service_client() as client:
				client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


@pytest.mark.unit
def test_process_task_success_path_with_refresh(monkeypatch):
	"""Successful stage execution should not fall into an error path."""

	task = QueueTask(
		id=123,
		dataset_id=456,
		user_id='test-user',
		task_types=[TaskTypeEnum.metadata],
		priority=1,
		is_processing=False,
		claimed_by='worker-a',
		current_position=1,
		estimated_time=0.0,
	)
	stage_calls = []
	deleted_filters = []
	processing_updates = []
	notification_calls = []

	class _DeleteQuery:
		def eq(self, field, value):
			deleted_filters.append((field, value))
			return self

		def execute(self):
			return None

	class _UpdateQuery:
		def __init__(self, payload):
			self.payload = payload

		def eq(self, field, value):
			assert field == 'id'
			assert self.payload == {'is_processing': True}
			processing_updates.append(value)
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

	monkeypatch.setattr(processor_module, 'verify_token', lambda token: {'id': 'processor-user'})
	monkeypatch.setattr(processor_module, 'refresh_processor_token', lambda task, token=None: 'refreshed-token')
	monkeypatch.setattr(processor_module, 'login', lambda username, password: 'final-token')
	monkeypatch.setattr(queue_runtime_module, 'use_client', lambda token: _FakeClient())
	monkeypatch.setattr(processor_module.logger, 'info', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module.logger, 'error', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module.logger, 'warning', lambda *args, **kwargs: None)
	monkeypatch.setattr(
		processor_module,
		'_notify_processing_result_safely',
		lambda current_task, event_type, token: notification_calls.append(
			(current_task.id, event_type, token, processor_module._inflight_task)
		),
	)
	monkeypatch.setattr(
		processor_module,
		'process_metadata',
		lambda current_task, processing_path: stage_calls.append((current_task.id, str(processing_path))),
	)

	process_task(task, 'initial-token')

	assert stage_calls == [(task.id, str(settings.processing_path))]
	assert processing_updates == []
	assert deleted_filters == [('id', task.id), ('claimed_by', 'worker-a')]
	assert notification_calls == [(task.id, ProcessingNotificationType.completed, 'final-token', None)]


@pytest.mark.unit
def test_queue_cleanup_failure_after_success_does_not_emit_failure_notification(monkeypatch):
	task = QueueTask(
		id=124,
		dataset_id=457,
		user_id='test-user',
		task_types=[TaskTypeEnum.metadata],
		priority=1,
		is_processing=True,
		claimed_by='worker-a',
		current_position=1,
	)
	notification_calls = []
	linear_calls = []

	def fail_queue_cleanup(token, current_task):
		raise RuntimeError('queue unavailable')

	monkeypatch.setattr(processor_module, 'verify_token', lambda token: {'id': 'processor-user'})
	monkeypatch.setattr(processor_module, 'refresh_processor_token', lambda task, token=None: 'refreshed-token')
	monkeypatch.setattr(processor_module, 'login', lambda username, password: 'final-token')
	monkeypatch.setattr(processor_module, 'process_metadata', lambda current_task, processing_path: None)
	monkeypatch.setattr(
		processor_module,
		'_notify_processing_result_safely',
		lambda current_task, event_type, token: notification_calls.append((event_type, token)),
	)
	monkeypatch.setattr(
		processor_module,
		'delete_queue_task',
		fail_queue_cleanup,
	)
	monkeypatch.setattr(
		processor_module,
		'create_processing_failure_issue',
		lambda **kwargs: linear_calls.append(kwargs),
	)

	with pytest.raises(RuntimeError, match='queue unavailable'):
		process_task(task, 'initial-token')

	assert notification_calls == [(ProcessingNotificationType.completed, 'final-token')]
	assert linear_calls == []


@pytest.mark.unit
def test_outbox_persistence_failure_keeps_completed_queue_task(monkeypatch):
	task = QueueTask(
		id=125,
		dataset_id=458,
		user_id='test-user',
		task_types=[TaskTypeEnum.metadata],
		priority=1,
		is_processing=True,
		claimed_by='worker-a',
		current_position=1,
	)
	deleted = []

	monkeypatch.setattr(processor_module, 'verify_token', lambda token: {'id': 'processor-user'})
	monkeypatch.setattr(processor_module, 'refresh_processor_token', lambda task, token=None: 'refreshed-token')
	monkeypatch.setattr(processor_module, 'login', lambda username, password: 'final-token')
	monkeypatch.setattr(processor_module, 'process_metadata', lambda current_task, processing_path: None)
	monkeypatch.setattr(
		processor_module,
		'_notify_processing_result_safely',
		lambda *args: (_ for _ in ()).throw(RuntimeError('outbox unavailable')),
	)
	monkeypatch.setattr(
		processor_module,
		'delete_queue_task',
		lambda token, current_task: deleted.append(current_task.id),
	)

	with pytest.raises(RuntimeError, match='outbox unavailable'):
		process_task(task, 'initial-token')

	assert deleted == []
	assert processor_module._inflight_task is None


@pytest.mark.unit
def test_outbox_persistence_failure_keeps_failed_queue_task(monkeypatch):
	task = QueueTask(
		id=127,
		dataset_id=460,
		user_id='test-user',
		task_types=[TaskTypeEnum.metadata],
		priority=1,
		is_processing=True,
		claimed_by='worker-a',
		current_position=1,
	)
	deleted = []
	status_updates = []

	monkeypatch.setattr(processor_module, 'verify_token', lambda token: {'id': 'processor-user'})
	monkeypatch.setattr(processor_module, 'refresh_processor_token', lambda task, token=None: 'refreshed-token')
	monkeypatch.setattr(processor_module, 'process_metadata', lambda *args: (_ for _ in ()).throw(RuntimeError('failed')))
	monkeypatch.setattr(processor_module, 'create_processing_failure_issue', lambda **kwargs: None)
	monkeypatch.setattr(processor_module, 'update_status', lambda *args, **kwargs: status_updates.append(kwargs))
	monkeypatch.setattr(
		processor_module,
		'_notify_processing_result_safely',
		lambda *args: (_ for _ in ()).throw(RuntimeError('outbox unavailable')),
	)
	monkeypatch.setattr(processor_module, 'delete_queue_task', lambda token, current_task: deleted.append(current_task.id))
	monkeypatch.setattr(processor_module.logger, 'info', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module.logger, 'error', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module.logger, 'warning', lambda *args, **kwargs: None)

	with pytest.raises(RuntimeError, match='outbox unavailable'):
		process_task(task, 'initial-token')

	assert deleted == []
	assert status_updates == [{
		'dataset_id': task.dataset_id,
		'current_status': StatusEnum.idle,
		'has_error': True,
		'error_message': 'metadata processing failed: failed',
		'error_stage': 'metadata',
	}]
	assert processor_module._inflight_task is None


@pytest.mark.unit
def test_failed_notification_recovery_does_not_block_waiting_work(monkeypatch):
	active_task = QueueTask(
		id=128,
		dataset_id=461,
		user_id='test-user',
		task_types=[TaskTypeEnum.metadata],
		priority=2,
		is_processing=True,
		claimed_by='worker-a',
		current_position=1,
	)
	waiting_task = QueueTask(
		id=129,
		dataset_id=462,
		user_id='test-user',
		task_types=[TaskTypeEnum.metadata],
		priority=1,
		is_processing=False,
		current_position=2,
	)
	processed = []
	crash_failures = []
	execution_order = []

	class FakeQuery:
		def select(self, columns):
			return self

		def eq(self, column, value):
			return self

		def execute(self):
			return SimpleNamespace(data=[{
				'current_status': 'idle',
				'has_error': True,
				'error_message': 'original stage failure',
			}])

	class FakeClient:
		def table(self, name):
			return FakeQuery()

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

	monkeypatch.setattr(processor_module.signal, 'signal', lambda *args: None)
	monkeypatch.setattr(processor_module, 'login_verified', lambda *args: ('token', object()))
	monkeypatch.setattr(
		processor_module,
		'_reconcile_processing_notifications_safely',
		lambda: execution_order.append('reconcile'),
	)
	monkeypatch.setattr(processor_module, 'get_worker_id', lambda: 'worker-a')
	monkeypatch.setattr(processor_module, 'get_active_task', lambda *args: active_task)
	monkeypatch.setattr(processor_module, 'get_next_task', lambda token: waiting_task)
	monkeypatch.setattr(processor_module, 'use_client', lambda token: FakeClient())
	monkeypatch.setattr(processor_module, '_kill_dangling_dataset_resources', lambda dataset_id: None)
	monkeypatch.setattr(processor_module, 'are_requested_stages_complete', lambda *args: False)
	monkeypatch.setattr(
		processor_module,
		'_fail_crashed_task',
		lambda *args: crash_failures.append(args),
	)
	monkeypatch.setattr(
		processor_module,
		'_notify_processing_result_safely',
		lambda *args: (_ for _ in ()).throw(RuntimeError('outbox unavailable')),
	)
	monkeypatch.setattr(processor_module, 'is_dataset_uploaded_or_processed', lambda *args: (True, False))
	monkeypatch.setattr(processor_module, 'claim_task', lambda *args: waiting_task)
	monkeypatch.setattr(
		processor_module,
		'process_task',
		lambda task, token: (execution_order.append('process'), processed.append(task.id)),
	)
	monkeypatch.setattr(processor_module.logger, 'info', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module.logger, 'warning', lambda *args, **kwargs: None)

	background_process()

	assert processed == [waiting_task.id]
	assert crash_failures == []
	assert execution_order == ['process', 'reconcile']


@pytest.mark.unit
def test_shutdown_during_completion_notification_does_not_release_task(monkeypatch):
	task = QueueTask(
		id=126,
		dataset_id=459,
		user_id='test-user',
		task_types=[TaskTypeEnum.metadata],
		priority=1,
		is_processing=True,
		claimed_by='worker-a',
		current_position=1,
	)
	released = []

	monkeypatch.setattr(processor_module, 'verify_token', lambda token: {'id': 'processor-user'})
	monkeypatch.setattr(processor_module, 'refresh_processor_token', lambda task, token=None: 'refreshed-token')
	monkeypatch.setattr(processor_module, 'login', lambda username, password: 'final-token')
	monkeypatch.setattr(processor_module, 'process_metadata', lambda current_task, processing_path: None)
	monkeypatch.setattr(
		processor_module,
		'release_queue_task',
		lambda token, current_task: released.append(current_task.id),
	)
	monkeypatch.setattr(
		processor_module,
		'_notify_processing_result_safely',
		lambda *args: processor_module._handle_graceful_shutdown(15, None),
	)

	with pytest.raises(SystemExit):
		process_task(task, 'initial-token')

	assert released == []
	assert processor_module._inflight_task is None
