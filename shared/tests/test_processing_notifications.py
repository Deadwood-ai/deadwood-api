from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

import pytest

from api.src.notifications import notify as notification_api
from shared.notifications import email as notification_email
from shared.notifications import processing as processing_notifications
from shared.models import QueueTask, TaskTypeEnum
from shared.notifications.processing import (
	ProcessingNotificationType,
	build_recipient_roles,
	is_dataset_user_visible_ready,
)
from shared.notifications.templates import (
	dataset_completed_email,
	dataset_failed_email,
	processing_failure_holiday_note_is_active,
)


def complete_status() -> dict:
	return {
		'current_status': 'idle',
		'is_upload_done': True,
		'is_odm_done': False,
		'is_ortho_done': True,
		'is_metadata_done': True,
		'is_cog_done': True,
		'is_thumbnail_done': True,
		'is_deadwood_done': True,
		'is_forest_cover_done': True,
		'is_combined_model_done': False,
		'is_aoi_done': False,
		'is_aoi_required': False,
		'is_embeddings_done': False,
		'has_error': False,
	}


@pytest.mark.unit
def test_user_visible_ready_requires_thumbnail_and_predictions():
	status = complete_status()
	assert is_dataset_user_visible_ready(status, 'forest.tif') is True
	assert is_dataset_user_visible_ready({**status, 'is_thumbnail_done': False}, 'forest.tif') is False
	assert is_dataset_user_visible_ready(
		{**status, 'is_deadwood_done': False, 'is_forest_cover_done': False},
		'forest.tif',
	) is False
	assert is_dataset_user_visible_ready(
		{**status, 'is_deadwood_done': False, 'is_forest_cover_done': False, 'is_combined_model_done': True},
		'forest.tif',
	) is True


@pytest.mark.unit
def test_user_visible_ready_handles_aoi_odm_and_embeddings():
	status = complete_status()
	assert is_dataset_user_visible_ready({**status, 'is_aoi_required': True}, 'forest.tif') is False
	assert is_dataset_user_visible_ready(
		{**status, 'is_aoi_required': True, 'is_aoi_done': True},
		'forest.tif',
	) is True
	assert is_dataset_user_visible_ready(status, 'raw-images.zip') is False
	assert is_dataset_user_visible_ready({**status, 'is_odm_done': True}, 'raw-images.zip') is True
	assert is_dataset_user_visible_ready(status, 'forest.tif') is True
	assert is_dataset_user_visible_ready(
		{**status, 'current_status': 'embedding_processing'},
		'forest.tif',
	) is False


@pytest.mark.unit
def test_owner_and_requester_are_deduplicated_with_roles():
	assert build_recipient_roles('owner', 'requester') == {
		'owner': ['owner'],
		'requester': ['requester'],
	}
	assert build_recipient_roles('same-user', 'same-user') == {
		'same-user': ['owner', 'requester'],
	}


@pytest.mark.unit
def test_requester_recipient_requires_current_authorization():
	class FakeQuery:
		def __init__(self, authorized):
			self.authorized = authorized

		def select(self, columns):
			return self

		def eq(self, column, value):
			return self

		def limit(self, value):
			return self

		def execute(self):
			return SimpleNamespace(data=[{'user_id': 'requester'}] if self.authorized else [])

	class FakeClient:
		def __init__(self, authorized):
			self.authorized = authorized

		def table(self, table):
			assert table == 'privileged_users'
			return FakeQuery(self.authorized)

	assert processing_notifications._requester_is_authorized(FakeClient(False), 'owner', 'requester') is False
	assert processing_notifications._requester_is_authorized(FakeClient(True), 'owner', 'requester') is True
	assert processing_notifications._requester_is_authorized(FakeClient(False), 'owner', 'owner') is True


@pytest.mark.unit
def test_templates_escape_user_controlled_content_and_use_canonical_route():
	unsafe_name = '<img src=x onerror=alert(1)>'
	internal_error = '<a href="https://example.invalid">internal</a>'

	_, _, failed_html = dataset_failed_email(123, unsafe_name, internal_error)
	_, _, completed_html = dataset_completed_email(123, unsafe_name)

	assert unsafe_name not in failed_html
	assert unsafe_name not in completed_html
	assert internal_error not in failed_html
	assert '&lt;img' in failed_html
	assert 'https://deadtrees.earth/dataset/123' in completed_html


@pytest.mark.unit
def test_failure_template_holiday_note_is_explicit_and_date_bounded():
	_, text_body, html_body = dataset_failed_email(123, 'forest.tif', today=date(2026, 9, 15))
	_, plain_text_body, plain_html_body = dataset_failed_email(123, 'forest.tif', today=date(2026, 9, 16))

	assert 'Most of our team are currently on holiday' in text_body
	assert 'Most of our team are currently on holiday' in html_body
	assert 'Most of our team are currently on holiday' not in plain_text_body
	assert 'Most of our team are currently on holiday' not in plain_html_body
	assert processing_failure_holiday_note_is_active(date(2026, 9, 15), today=date(2026, 9, 15)) is True
	assert processing_failure_holiday_note_is_active(date(2026, 9, 15), today=date(2026, 9, 16)) is False
	assert processing_failure_holiday_note_is_active(None, today=date(2026, 8, 7)) is False


@pytest.mark.unit
def test_failure_event_render_uses_configured_holiday_cutoff(monkeypatch):
	monkeypatch.setattr(
		processing_notifications.settings,
		'PROCESSING_FAILURE_EMAIL_HOLIDAY_NOTE_UNTIL',
		date(9999, 12, 31),
	)
	_, text_body, _ = processing_notifications._render_event(
		ProcessingNotificationType.failed,
		123,
		'forest.tif',
	)

	assert 'Most of our team are currently on holiday' in text_body


@pytest.mark.unit
def test_failure_delivery_paths_render_identical_bodies_at_holiday_cutoff(monkeypatch):
	captured = {}

	def capture_email(to_email, subject, html_body, *, text_body=None):
		captured['message'] = (subject, text_body, html_body)
		return {'success': True}

	monkeypatch.setattr(
		notification_api.settings,
		'PROCESSING_FAILURE_EMAIL_HOLIDAY_NOTE_UNTIL',
		date.today(),
	)
	monkeypatch.setattr(notification_api, 'send_email', capture_email)

	result = notification_api.notify_dataset_failed(
		dataset_id=999,
		error_message='internal failure',
		to_email='owner@example.com',
		file_name='my_ortho.tif',
	)
	processor_message = processing_notifications._render_event(
		ProcessingNotificationType.failed,
		999,
		'my_ortho.tif',
	)

	assert result == {'success': True}
	assert captured['message'] == processor_message
	assert 'Most of our team are currently on holiday' in captured['message'][1]


@pytest.mark.unit
def test_brevo_delivery_uses_event_id_as_idempotency_key(monkeypatch):
	requests = []

	class FakeResponse:
		status_code = 201
		text = ''

		@staticmethod
		def json():
			return {'messageId': 'brevo-message'}

	class FakeClient:
		def __init__(self, timeout):
			assert timeout == 15

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

		@staticmethod
		def post(url, json, headers):
			requests.append((url, json, headers))
			return FakeResponse()

	monkeypatch.setattr(notification_email.settings, 'DEV_MODE', False)
	monkeypatch.setattr(notification_email.settings, 'BREVO_API_KEY', 'test-api-key')
	monkeypatch.setattr(notification_email.httpx, 'Client', FakeClient)

	result = notification_email.send_email(
		'user@example.com',
		'Subject',
		'<p>Body</p>',
		text_body='Body',
		idempotency_key='2d69dca7-e5de-4736-9e1a-bfb5a59811e4',
	)

	assert result == {'success': True, 'message_id': 'brevo-message', 'method': 'brevo'}
	assert requests[0][1]['headers'] == {
		'idempotencyKey': '2d69dca7-e5de-4736-9e1a-bfb5a59811e4'
	}


@pytest.mark.unit
def test_brevo_duplicate_idempotency_response_counts_as_delivered(monkeypatch):
	class FakeResponse:
		status_code = 400
		text = '{"code":"duplicate_parameter","message":"duplicate idempotencyKey"}'

	class FakeClient:
		def __init__(self, timeout):
			assert timeout == 15

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

		@staticmethod
		def post(url, json, headers):
			assert json['headers'] == {'idempotencyKey': 'stable-event-id'}
			return FakeResponse()

	monkeypatch.setattr(notification_email.settings, 'DEV_MODE', False)
	monkeypatch.setattr(notification_email.settings, 'BREVO_API_KEY', 'test-api-key')
	monkeypatch.setattr(notification_email.httpx, 'Client', FakeClient)

	result = notification_email.send_email(
		'user@example.com',
		'Subject',
		'<p>Body</p>',
		idempotency_key='stable-event-id',
	)

	assert result == {
		'success': True,
		'message_id': None,
		'method': 'brevo',
		'duplicate': True,
	}


@pytest.mark.unit
def test_brevo_failure_does_not_return_provider_response_body(monkeypatch):
	class FakeResponse:
		status_code = 503
		text = 'sensitive provider response'

	class FakeClient:
		def __init__(self, timeout):
			assert timeout == 15

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

		@staticmethod
		def post(url, json, headers):
			return FakeResponse()

	monkeypatch.setattr(notification_email.settings, 'DEV_MODE', False)
	monkeypatch.setattr(notification_email.settings, 'BREVO_API_KEY', 'test-api-key')
	monkeypatch.setattr(notification_email.httpx, 'Client', FakeClient)

	result = notification_email.send_email(
		'user@example.com',
		'Subject',
		'<p>Body</p>',
	)

	assert result == {
		'success': False,
		'error': 'brevo_api_error_503',
		'method': 'brevo',
	}


@pytest.mark.unit
def test_delivery_retry_refreshes_recipient_email(monkeypatch):
	claimed = {
		'id': 'event-id',
		'dataset_id': 123,
		'event_type': ProcessingNotificationType.failed.value,
		'recipient_user_id': 'user-id',
		'recipient_email': 'old@example.com',
		'delivery_attempts': 2,
	}
	sent_to = []
	updates = []

	monkeypatch.setattr(processing_notifications.settings, 'PROCESSING_EMAIL_NOTIFICATIONS_ENABLED', True)
	monkeypatch.setattr(processing_notifications, '_claim_event', lambda client, event: claimed)
	monkeypatch.setattr(processing_notifications, '_load_preferences', lambda client, user_ids: {})
	monkeypatch.setattr(processing_notifications, '_get_user_email', lambda client, user_id: 'new@example.com')
	monkeypatch.setattr(
		processing_notifications,
		'send_email',
		lambda to_email, *args, **kwargs: (
			sent_to.append(to_email) or {'success': True, 'message_id': 'message-id', 'method': 'brevo'}
		),
	)
	monkeypatch.setattr(
		processing_notifications,
		'_update_event',
		lambda client, event_id, update: updates.append(update) or update,
	)
	monkeypatch.setattr(processing_notifications, '_recipient_is_still_authorized', lambda client, event: True)

	processing_notifications._dispatch_event(object(), claimed, 'forest.tif')

	assert sent_to == ['new@example.com']
	assert updates[0]['recipient_email'] == 'new@example.com'


@pytest.mark.unit
def test_disabled_notifications_do_not_require_service_access(monkeypatch):
	def fail_if_service_client_is_used():
		raise AssertionError('disabled notifications must not access the service client')

	monkeypatch.setattr(processing_notifications, 'use_service_client', fail_if_service_client_is_used)
	monkeypatch.setattr(
		processing_notifications.settings,
		'PROCESSING_EMAIL_NOTIFICATIONS_ENABLED',
		False,
	)
	task = QueueTask(
		id=123,
		dataset_id=456,
		user_id='owner-user',
		priority=2,
		is_processing=True,
		current_position=1,
		task_types=[TaskTypeEnum.metadata],
	)

	result = processing_notifications.notify_processing_result(
		task,
		ProcessingNotificationType.failed,
	)

	assert result == []


@pytest.mark.unit
def test_failed_notification_handles_missing_status_response(monkeypatch):
	event_payloads = []

	class FakeQuery:
		def __init__(self, table):
			self.table = table

		def select(self, columns):
			return self

		def eq(self, column, value):
			return self

		def single(self):
			return self

		def maybe_single(self):
			return self

		def in_(self, column, values):
			return self

		def upsert(self, payload, **kwargs):
			event_payloads.extend(payload)
			return self

		def execute(self):
			if self.table == processing_notifications.settings.datasets_table:
				return SimpleNamespace(data={'user_id': 'owner-user', 'file_name': 'forest.tif'})
			if self.table == processing_notifications.settings.statuses_table:
				return None
			if self.table == processing_notifications.settings.notification_preferences_table:
				return SimpleNamespace(data=[])
			if self.table == processing_notifications.settings.processing_notification_events_table:
				return SimpleNamespace(data=[{'id': 'event-id', **event} for event in event_payloads])
			raise AssertionError(f'Unexpected table: {self.table}')

	class FakeClient:
		def table(self, table):
			return FakeQuery(table)

	@contextmanager
	def use_fake_service_client():
		yield FakeClient()

	monkeypatch.setattr(processing_notifications, 'use_service_client', use_fake_service_client)
	monkeypatch.setattr(processing_notifications, '_get_user_email', lambda client, user_id: 'owner@example.com')
	monkeypatch.setattr(processing_notifications.settings, 'PROCESSING_EMAIL_NOTIFICATIONS_ENABLED', True)
	task = QueueTask(
		id=123,
		dataset_id=456,
		user_id='owner-user',
		priority=2,
		is_processing=True,
		current_position=1,
		task_types=[TaskTypeEnum.metadata],
	)

	batch = processing_notifications.record_processing_result(task, ProcessingNotificationType.failed)

	assert len(batch.events) == 1
	assert batch.events[0]['status'] == 'pending'
	assert batch.events[0]['status_snapshot'] == {}


@pytest.mark.unit
def test_partial_success_returns_empty_notification_batch(monkeypatch):
	class FakeQuery:
		def __init__(self, table):
			self.table = table

		def select(self, columns):
			return self

		def eq(self, column, value):
			return self

		def single(self):
			return self

		def maybe_single(self):
			return self

		def execute(self):
			if self.table == processing_notifications.settings.datasets_table:
				return SimpleNamespace(data={'user_id': 'owner-user', 'file_name': 'forest.tif'})
			if self.table == processing_notifications.settings.statuses_table:
				return SimpleNamespace(data={**complete_status(), 'is_thumbnail_done': False})
			raise AssertionError(f'Unexpected table: {self.table}')

	class FakeClient:
		def table(self, table):
			return FakeQuery(table)

	@contextmanager
	def use_fake_service_client():
		yield FakeClient()

	monkeypatch.setattr(processing_notifications, 'use_service_client', use_fake_service_client)
	monkeypatch.setattr(processing_notifications.settings, 'PROCESSING_EMAIL_NOTIFICATIONS_ENABLED', True)
	task = QueueTask(
		id=124,
		dataset_id=457,
		user_id='owner-user',
		priority=2,
		is_processing=True,
		current_position=1,
		task_types=[TaskTypeEnum.metadata],
	)

	batch = processing_notifications.record_processing_result(
		task,
		ProcessingNotificationType.completed,
	)

	assert batch.events == []
	assert batch.file_name == 'forest.tif'


@pytest.mark.unit
def test_failure_email_links_to_status_without_unsupported_retry_or_diagnostics():
	from shared.notifications.templates import dataset_failed_email
	_, text, html = dataset_failed_email(123, '<forest>.tif', error_message='secret internal path')
	for body in (text, html):
		assert 'https://deadtrees.earth/profile?dataset=123' in body
		assert 'info@deadtrees.earth' in body
		assert 'retry processing from your account' not in body
		assert 'secret internal path' not in body
	assert '&lt;forest&gt;.tif' in html
