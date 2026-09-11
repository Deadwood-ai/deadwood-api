import pytest
from pathlib import Path
from types import SimpleNamespace

import processor.src.processor as processor_module
import processor.src.utils.queue_runtime as queue_runtime_module
from shared.db import use_client
from shared.settings import settings
from shared.models import TaskTypeEnum, QueueTask, StatusEnum
from processor.src.utils.drain_control import BackgroundProcessResult
from shared.notifications.processing import ProcessingNotificationType
from processor.src.processor import (
	background_process, process_task, get_next_task,
	detect_crashed_stage, get_completed_stages, are_requested_stages_complete, PIPELINE_STAGE_MAP,
)


@pytest.fixture
def processor_task(test_dataset_for_processing, test_processor_user, auth_token):
	"""Create a test task for processor testing"""
	task_id = None
	try:
		# Create test task in queue
		with use_client(auth_token) as client:
			task_data = {
				'dataset_id': test_dataset_for_processing,
				'user_id': test_processor_user,
				'task_types': [TaskTypeEnum.metadata],
				'priority': 1,
			}
			response = client.table(settings.queue_table).insert(task_data).execute()
			task_id = response.data[0]['id']

			yield task_id

	finally:
		# Cleanup
		if task_id:
			with use_client(auth_token) as client:
				client.table(settings.queue_table).delete().eq('id', task_id).execute()


def test_background_process_success(processor_task, auth_token, test_dataset_for_processing):
	"""Test successful background processing of a task"""
	# Run the background process
	background_process()

	# Verify task was processed and removed from queue
	with use_client(auth_token) as client:
		# Check queue is empty
		queue_response = client.table(settings.queue_table).select('*').eq('id', processor_task).execute()
		assert len(queue_response.data) == 0

		# Check status was updated
		status_response = (
			client.table(settings.statuses_table).select('*').eq('dataset_id', test_dataset_for_processing).execute()
		)
		assert len(status_response.data) == 1
		status = status_response.data[0]

		# Verify status updates
		assert status['current_status'] == StatusEnum.idle
		assert status['is_metadata_done'] is True
		assert not status['has_error']


def test_background_process_no_tasks():
	"""Test background process behavior when no tasks are in queue"""
	# Run the background process with empty queue
	background_process()

	# Verify it reports that no work was done when the queue is empty, so the
	# continuous worker knows to back off instead of hot-looping.
	assert background_process() is BackgroundProcessResult.IDLE


@pytest.mark.unit
def test_background_process_returns_false_while_drained(monkeypatch):
	queue_reads = []
	monkeypatch.setattr(processor_module.signal, 'signal', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module, 'login_verified', lambda username, password: ('token', object()))
	monkeypatch.setattr(processor_module, 'get_worker_id', lambda: 'worker-a')
	monkeypatch.setattr(processor_module, 'get_active_task', lambda token, worker_id: None)
	monkeypatch.setattr(processor_module, 'is_drain_requested', lambda: True)
	monkeypatch.setattr(processor_module, 'acknowledge_drain_request', lambda worker_id: None)
	monkeypatch.setattr(processor_module, 'get_next_task', lambda token: queue_reads.append(token) or None)

	assert background_process() is BackgroundProcessResult.IDLE
	assert queue_reads == ['token']


@pytest.mark.unit
def test_background_process_returns_true_when_claimed_task_fails(monkeypatch):
	task = QueueTask(
		id=123,
		dataset_id=456,
		user_id='test-user',
		task_types=[TaskTypeEnum.metadata],
		priority=1,
		is_processing=True,
		claimed_by='worker-a',
		current_position=1,
		estimated_time=0.0,
	)
	logger_messages = []
	health_events = []

	class _SelectQuery:
		def eq(self, field, value):
			return self

		def execute(self):
			return type('Response', (), {'data': [{'current_status': 'idle'}]})()

	class _FakeClient:
		def table(self, name):
			assert name == settings.statuses_table
			return type('Table', (), {'select': lambda self, fields: _SelectQuery()})()

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc, tb):
			return False

	monkeypatch.setattr(processor_module.signal, 'signal', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module, 'login_verified', lambda username, password: ('token', object()))
	monkeypatch.setattr(processor_module, 'get_worker_id', lambda: 'worker-a')
	monkeypatch.setattr(processor_module, 'get_active_task', lambda token, worker_id: None)
	monkeypatch.setattr(processor_module, 'clear_loop_unhealthy', lambda: health_events.append('healthy'))
	monkeypatch.setattr(processor_module, 'is_drain_requested', lambda: False)
	monkeypatch.setattr(processor_module, 'get_next_task', lambda token: task)
	monkeypatch.setattr(processor_module, 'is_dataset_uploaded_or_processed', lambda task, token: (True, False))
	monkeypatch.setattr(processor_module, 'claim_task', lambda token, task, worker_id: task)
	monkeypatch.setattr(processor_module, 'use_client', lambda token: _FakeClient())
	monkeypatch.setattr(
		processor_module,
		'process_task',
		lambda task, token: health_events.append('processing') or (_ for _ in ()).throw(RuntimeError('boom')),
	)
	monkeypatch.setattr(processor_module.logger, 'info', lambda *args, **kwargs: logger_messages.append(args[0]))

	assert background_process() is BackgroundProcessResult.FAILED
	assert health_events == ['healthy', 'processing']
	assert any('Start processing queued task' in message for message in logger_messages)


@pytest.mark.unit
def test_background_process_releases_claim_when_drain_arrives_after_claim(monkeypatch):
	task = QueueTask(
		id=123,
		dataset_id=456,
		user_id='test-user',
		task_types=[TaskTypeEnum.metadata],
		priority=1,
		is_processing=True,
		claimed_by='worker-a',
		current_position=1,
		estimated_time=0.0,
	)
	released = []
	drain_checks = iter([False, False, True])

	monkeypatch.setattr(processor_module.signal, 'signal', lambda *args, **kwargs: None)
	monkeypatch.setattr(processor_module, 'login_verified', lambda username, password: ('token', object()))
	monkeypatch.setattr(processor_module, 'get_worker_id', lambda: 'worker-a')
	monkeypatch.setattr(processor_module, 'get_active_task', lambda token, worker_id: None)
	monkeypatch.setattr(processor_module, 'is_drain_requested', lambda: next(drain_checks))
	monkeypatch.setattr(processor_module, 'acknowledge_drain_request', lambda worker_id: None)
	monkeypatch.setattr(processor_module, 'get_next_task', lambda token: task)
	monkeypatch.setattr(processor_module, 'is_dataset_uploaded_or_processed', lambda task, token: (True, False))
	monkeypatch.setattr(processor_module, 'claim_task', lambda token, task, worker_id: task)
	monkeypatch.setattr(processor_module, 'release_queue_task', lambda token, task: released.append(task.id))
	monkeypatch.setattr(
		processor_module,
		'process_task',
		lambda task, token: pytest.fail('drained worker should release the claim before starting work'),
	)

	assert background_process() is BackgroundProcessResult.IDLE
	assert released == [task.id]


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
		'process_metadata',
		lambda current_task, processing_path: stage_calls.append((current_task.id, str(processing_path))),
	)

	process_task(task, 'initial-token')

	assert stage_calls == [(task.id, str(settings.processing_path))]
	assert processing_updates == []
	assert deleted_filters == [('id', task.id), ('claimed_by', 'worker-a')]


@pytest.mark.unit
def test_pipeline_stage_map_is_stable_and_ordered():
	"""
	Locks down the pipeline stage ordering used for crash detection and reporting.
	If this changes, it should be intentional (and reviewed), because it affects
	how we attribute failures to stages.
	"""
	expected = [
		(TaskTypeEnum.odm_processing, 'is_odm_done', 'odm_processing'),
		(TaskTypeEnum.geotiff, 'is_ortho_done', 'ortho_processing'),
		(TaskTypeEnum.metadata, 'is_metadata_done', 'metadata_processing'),
		(TaskTypeEnum.cog, 'is_cog_done', 'cog_processing'),
		(TaskTypeEnum.thumbnail, 'is_thumbnail_done', 'thumbnail_processing'),
		(TaskTypeEnum.deadwood_v1, 'is_deadwood_done', 'deadwood_segmentation'),
		(TaskTypeEnum.treecover_v1, 'is_forest_cover_done', 'forest_cover_segmentation'),
		(
			TaskTypeEnum.deadwood_treecover_combined_v2,
			'is_combined_model_done',
			'deadwood_treecover_combined_segmentation',
		),
		(TaskTypeEnum.aoi_v1, 'is_aoi_done', 'aoi_segmentation'),
		(TaskTypeEnum.embeddings_v1, 'is_embeddings_done', 'embedding_processing'),
	]
	assert PIPELINE_STAGE_MAP == expected


@pytest.fixture
def sequential_task(test_dataset_for_processing, test_processor_user):
	"""Create a test task for sequential processing"""
	return QueueTask(
		id=1,
		dataset_id=test_dataset_for_processing,
		user_id=test_processor_user,
		task_types=[
			TaskTypeEnum.geotiff,
			TaskTypeEnum.cog,
			TaskTypeEnum.thumbnail,
			TaskTypeEnum.metadata,
		],
		priority=1,
		is_processing=False,  # Column still exists in DB but is inert
		current_position=1,
		estimated_time=0.0,
	)


@pytest.mark.integration
@pytest.mark.usefixtures('ensure_metadata_support_data')
def test_sequential_processing(sequential_task, auth_token):
	"""Test running all processing steps sequentially"""
	# Process all tasks
	process_task(sequential_task, auth_token)

	# Verify results in database
	with use_client(auth_token) as client:
		# Check GeoTIFF processing
		ortho_response = (
			client.table(settings.orthos_processed_table)
			.select('*')
			.eq('dataset_id', sequential_task.dataset_id)
			.execute()
		)
		assert len(ortho_response.data) == 1
		assert ortho_response.data[0]['ortho_processing_runtime'] > 0

		# Check COG processing
		cog_response = (
			client.table(settings.cogs_table).select('*').eq('dataset_id', sequential_task.dataset_id).execute()
		)
		assert len(cog_response.data) == 1
		assert cog_response.data[0]['cog_file_size'] > 0
		assert cog_response.data[0]['cog_info'] is not None

		# Check thumbnail processing
		thumbnail_response = (
			client.table(settings.thumbnails_table).select('*').eq('dataset_id', sequential_task.dataset_id).execute()
		)
		assert len(thumbnail_response.data) == 1
		assert thumbnail_response.data[0]['thumbnail_file_size'] > 0
		assert thumbnail_response.data[0]['thumbnail_processing_runtime'] > 0

		# Check metadata processing
		metadata_response = (
			client.table(settings.metadata_table).select('*').eq('dataset_id', sequential_task.dataset_id).execute()
		)
		assert len(metadata_response.data) == 1
		assert metadata_response.data[0]['processing_runtime'] > 0
		assert 'gadm' in metadata_response.data[0]['metadata']
		assert 'biome' in metadata_response.data[0]['metadata']

		# Verify final status
		status_response = (
			client.table(settings.statuses_table).select('*').eq('dataset_id', sequential_task.dataset_id).execute()
		)
		assert len(status_response.data) == 1
		status = status_response.data[0]
		assert status['current_status'] == StatusEnum.idle
		assert status['is_ortho_done'] is True
		assert status['is_cog_done'] is True
		assert status['is_thumbnail_done'] is True
		assert status['is_metadata_done'] is True
		assert not status['has_error']

		# Verify task was removed from queue
		queue_response = client.table(settings.queue_table).select('*').eq('id', sequential_task.id).execute()
		assert len(queue_response.data) == 0


@pytest.fixture
def processor_task_with_missing_file(test_processor_user, auth_token):
	"""Create a test task with a non-existent dataset file"""
	task_id = None
	try:
		# Create a dataset entry that points to a non-existent file
		with use_client(auth_token) as client:
			# First create dataset
			dataset_data = {
				'file_name': 'non_existent_file.tif',
				'user_id': test_processor_user,
				'license': 'CC BY',
				'platform': 'drone',
				'authors': ['Test Author'],
				'data_access': 'public',
				'aquisition_year': 2024,
				'aquisition_month': 1,
				'aquisition_day': 1,
			}
			dataset_response = client.table(settings.datasets_table).insert(dataset_data).execute()
			dataset_id = dataset_response.data[0]['id']

			# Create status entry
			status_data = {
				'dataset_id': dataset_id,
				'is_upload_done': True,
				'current_status': StatusEnum.idle,
			}
			client.table(settings.statuses_table).insert(status_data).execute()

			# Create test task in queue
			task_data = {
				'dataset_id': dataset_id,
				'user_id': test_processor_user,
				'task_types': [TaskTypeEnum.metadata],
				'priority': 1,
			}
			response = client.table(settings.queue_table).insert(task_data).execute()
			task_id = response.data[0]['id']

			yield task_id

	finally:
		# Cleanup
		if task_id:
			with use_client(auth_token) as client:
				# Get dataset_id before deleting task
				task_response = client.table(settings.queue_table).select('dataset_id').eq('id', task_id).execute()
				dataset_id = task_response.data[0]['dataset_id'] if task_response.data else None

				# Delete task
				client.table(settings.queue_table).delete().eq('id', task_id).execute()

				if dataset_id:
					# Delete status and dataset
					client.table(settings.statuses_table).delete().eq('dataset_id', dataset_id).execute()
					client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


@pytest.mark.integration
def test_failed_process_removes_task_from_queue(processor_task_with_missing_file, auth_token):
	"""Test that failed processing removes task from queue but records error in status.

	This prevents endless retry loops - the error is recorded in v2_statuses
	so users can see what failed, but the task doesn't block the queue.
	"""
	# First verify task exists before processing
	with use_client(auth_token) as client:
		initial_task = (
			client.table(settings.queue_table).select('*').eq('id', processor_task_with_missing_file).execute()
		)
		dataset_id = initial_task.data[0]['dataset_id']

	try:
		# Run the background process - this should raise a ProcessingError
		background_process()
	except Exception:
		# We expect an error
		pass

	# Verify task state after failed processing
	with use_client(auth_token) as client:
		# Task should be REMOVED from queue (prevents endless retry loop)
		queue_response = (
			client.table(settings.queue_table).select('*').eq('id', processor_task_with_missing_file).execute()
		)
		assert len(queue_response.data) == 0, 'Failed task should be removed from queue'

		# Check status was updated to reflect error
		status_response = client.table(settings.statuses_table).select('*').eq('dataset_id', dataset_id).execute()
		assert len(status_response.data) == 1
		status = status_response.data[0]

		# Verify error is recorded in status table
		assert status['has_error'] is True, 'Status should have has_error=True'
		assert status['error_message'] is not None, 'Error message should be recorded'


def test_processor_respects_priority(test_dataset_for_processing, test_processor_user, auth_token):
	"""Test that processor picks highest priority task first"""
	task_ids = []
	try:
		# Create two tasks with different priorities
		with use_client(auth_token) as client:
			# Create lower priority task first
			task1_data = {
				'dataset_id': test_dataset_for_processing,
				'user_id': test_processor_user,
				'task_types': [TaskTypeEnum.metadata],
				'priority': 2,  # Lower priority
			}
			response = client.table(settings.queue_table).insert(task1_data).execute()
			task_ids.append(response.data[0]['id'])

			# Create higher priority task second
			task2_data = {
				'dataset_id': test_dataset_for_processing,
				'user_id': test_processor_user,
				'task_types': [TaskTypeEnum.metadata],
				'priority': 5,  # Higher priority (changed from 1)
			}
			response = client.table(settings.queue_table).insert(task2_data).execute()
			task_ids.append(response.data[0]['id'])

		# Get next task
		next_task = get_next_task(auth_token)

		# Verify the higher priority task (priority=5) is selected first
		assert next_task is not None
		assert next_task.priority == 5  # Changed from 1 to 5

	finally:
		# Cleanup
		with use_client(auth_token) as client:
			for task_id in task_ids:
				client.table(settings.queue_table).delete().eq('id', task_id).execute()


# --- Crash detection unit tests ---

def test_detect_crashed_stage_finds_first_incomplete():
	"""Test that detect_crashed_stage returns the first incomplete stage in the pipeline."""
	status_data = {
		'is_odm_done': True,
		'is_ortho_done': True,
		'is_metadata_done': True,
		'is_cog_done': False,
		'is_thumbnail_done': False,
		'is_deadwood_done': False,
		'is_forest_cover_done': False,
	}
	task_types = [
		TaskTypeEnum.geotiff, TaskTypeEnum.metadata, TaskTypeEnum.cog,
		TaskTypeEnum.thumbnail, TaskTypeEnum.deadwood_v1, TaskTypeEnum.treecover_v1,
	]
	assert detect_crashed_stage(status_data, task_types) == 'cog_processing'


def test_detect_crashed_stage_only_checks_requested_types():
	"""Test that detect_crashed_stage only considers task types that were actually requested."""
	status_data = {
		'is_ortho_done': True,
		'is_metadata_done': True,
		'is_cog_done': True,
		'is_thumbnail_done': True,
		'is_deadwood_done': False,  # Not done, but not requested
		'is_forest_cover_done': False,
	}
	# Only requesting up to thumbnail -- deadwood/treecover not in the list
	task_types = [TaskTypeEnum.geotiff, TaskTypeEnum.metadata, TaskTypeEnum.cog, TaskTypeEnum.thumbnail]
	assert detect_crashed_stage(status_data, task_types) == 'unknown'


def test_detect_crashed_stage_deadwood():
	"""Test crash detection specifically for deadwood segmentation crash."""
	status_data = {
		'is_ortho_done': True,
		'is_metadata_done': True,
		'is_cog_done': True,
		'is_thumbnail_done': True,
		'is_deadwood_done': False,  # Crashed here
		'is_forest_cover_done': False,
		'current_status': 'deadwood_segmentation',
	}
	task_types = [
		TaskTypeEnum.geotiff, TaskTypeEnum.metadata, TaskTypeEnum.cog,
		TaskTypeEnum.thumbnail, TaskTypeEnum.deadwood_v1, TaskTypeEnum.treecover_v1,
	]
	assert detect_crashed_stage(status_data, task_types) == 'deadwood_segmentation'


def test_get_completed_stages():
	"""Test that get_completed_stages returns all completed stages."""
	status_data = {
		'is_odm_done': False,
		'is_ortho_done': True,
		'is_metadata_done': True,
		'is_cog_done': True,
		'is_thumbnail_done': True,
		'is_deadwood_done': False,
		'is_forest_cover_done': False,
	}
	completed = get_completed_stages(status_data)
	assert 'ortho_processing' in completed
	assert 'metadata_processing' in completed
	assert 'cog_processing' in completed
	assert 'thumbnail_processing' in completed
	assert 'deadwood_segmentation' not in completed
	assert 'forest_cover_segmentation' not in completed


def test_get_completed_stages_none_completed():
	"""Test get_completed_stages when nothing has completed."""
	status_data = {}
	completed = get_completed_stages(status_data)
	assert completed == []


def test_are_requested_stages_complete_only_returns_true_when_all_requested_flags_are_done():
	status_data = {
		'is_ortho_done': True,
		'is_metadata_done': True,
		'is_cog_done': False,
	}
	assert are_requested_stages_complete(status_data, [TaskTypeEnum.geotiff, TaskTypeEnum.metadata]) is True
	assert are_requested_stages_complete(status_data, [TaskTypeEnum.geotiff, TaskTypeEnum.cog]) is False
	assert are_requested_stages_complete(status_data, []) is False


def test_combined_stage_requires_combined_model_flag():
	status_data = {
		'is_deadwood_done': True,
		'is_forest_cover_done': True,
		'is_combined_model_done': False,
	}

	assert (
		detect_crashed_stage(status_data, [TaskTypeEnum.deadwood_treecover_combined_v2])
		== 'deadwood_treecover_combined_segmentation'
	)
	assert are_requested_stages_complete(status_data, [TaskTypeEnum.deadwood_treecover_combined_v2]) is False

	status_data['is_combined_model_done'] = True
	assert detect_crashed_stage(status_data, [TaskTypeEnum.deadwood_treecover_combined_v2]) == 'unknown'
	assert are_requested_stages_complete(status_data, [TaskTypeEnum.deadwood_treecover_combined_v2]) is True
	assert 'deadwood_treecover_combined_segmentation' in get_completed_stages(status_data)


def test_mixed_legacy_and_combined_recovery_requires_combined_model_flag():
	status_data = {
		'is_deadwood_done': True,
		'is_forest_cover_done': True,
		'is_combined_model_done': False,
	}
	task_types = [
		TaskTypeEnum.deadwood_v1,
		TaskTypeEnum.treecover_v1,
		TaskTypeEnum.deadwood_treecover_combined_v2,
	]

	assert are_requested_stages_complete(status_data, task_types) is False
	assert (
		detect_crashed_stage(status_data, task_types)
		== 'deadwood_treecover_combined_segmentation'
	)
	assert 'deadwood_segmentation' in get_completed_stages(status_data)
	assert 'forest_cover_segmentation' in get_completed_stages(status_data)
	assert 'deadwood_treecover_combined_segmentation' not in get_completed_stages(status_data)


def test_mixed_legacy_and_combined_recovery_accepts_combined_model_flag():
	status_data = {
		'is_deadwood_done': True,
		'is_forest_cover_done': True,
		'is_combined_model_done': True,
	}
	task_types = [
		TaskTypeEnum.deadwood_v1,
		TaskTypeEnum.treecover_v1,
		TaskTypeEnum.deadwood_treecover_combined_v2,
	]

	assert are_requested_stages_complete(status_data, task_types) is True
	assert detect_crashed_stage(status_data, task_types) == 'unknown'
	assert 'deadwood_treecover_combined_segmentation' in get_completed_stages(status_data)


@pytest.fixture
def crashed_dataset_task(test_processor_user, auth_token):
	"""Create a task that simulates a previous crash (current_status stuck, some stages done)."""
	task_id = None
	dataset_id = None
	try:
		with use_client(auth_token) as client:
			# Create dataset
			dataset_data = {
				'file_name': 'crash_test_file.tif',
				'user_id': test_processor_user,
				'license': 'CC BY',
				'platform': 'drone',
				'authors': ['Test Author'],
				'data_access': 'public',
				'aquisition_year': 2024,
				'aquisition_month': 1,
				'aquisition_day': 1,
			}
			dataset_response = client.table(settings.datasets_table).insert(dataset_data).execute()
			dataset_id = dataset_response.data[0]['id']

			# Create status entry that simulates a crash during deadwood segmentation
			status_data = {
				'dataset_id': dataset_id,
				'is_upload_done': True,
				'current_status': 'deadwood_segmentation',  # Stuck in non-idle state
				'is_ortho_done': True,
				'is_metadata_done': True,
				'is_cog_done': True,
				'is_thumbnail_done': True,
				'is_deadwood_done': False,  # Crashed before completing
				'is_forest_cover_done': False,
				'has_error': False,
			}
			client.table(settings.statuses_table).insert(status_data).execute()

			# Create queue task
			task_data = {
				'dataset_id': dataset_id,
				'user_id': test_processor_user,
				'task_types': [
					TaskTypeEnum.geotiff, TaskTypeEnum.metadata, TaskTypeEnum.cog,
					TaskTypeEnum.thumbnail, TaskTypeEnum.deadwood_v1, TaskTypeEnum.treecover_v1,
				],
				'priority': 1,
				'is_processing': True,
			}
			response = client.table(settings.queue_table).insert(task_data).execute()
			task_id = response.data[0]['id']

			yield {'task_id': task_id, 'dataset_id': dataset_id}

	finally:
		if auth_token:
			with use_client(auth_token) as client:
				if task_id:
					client.table(settings.queue_table).delete().eq('id', task_id).execute()
				if dataset_id:
					client.table(settings.statuses_table).delete().eq('dataset_id', dataset_id).execute()
					client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


def test_background_process_fails_crashed_dataset(crashed_dataset_task, auth_token, monkeypatch):
	"""A stale active task left mid-stage means the previous run was hard-killed
	(SIGKILL/OOM) without a graceful shutdown. Such crashes are deterministic, so
	the task is marked errored and removed from the queue rather than retried."""
	monkeypatch.setattr(processor_module, '_kill_dangling_dataset_resources', lambda dataset_id: None)

	linear_calls = []
	notification_calls = []
	monkeypatch.setattr(
		processor_module, 'create_processing_failure_issue',
		lambda **kwargs: linear_calls.append(kwargs),
	)
	monkeypatch.setattr(
		processor_module,
		'_notify_processing_result_safely',
		lambda task, event_type, token: notification_calls.append((task.id, event_type)),
	)

	dataset_id = crashed_dataset_task['dataset_id']

	background_process()

	with use_client(auth_token) as client:
		queue_response = (
			client.table(settings.queue_table).select('*')
			.eq('id', crashed_dataset_task['task_id']).execute()
		)
		assert len(queue_response.data) == 0, 'Crashed task should be removed from queue (no retry)'

		status_response = (
			client.table(settings.statuses_table).select('*')
			.eq('dataset_id', dataset_id).execute()
		)
		assert len(status_response.data) == 1
		status = status_response.data[0]
		assert status['has_error'] is True, 'Crashed task should be marked as error'
		assert status['current_status'] == 'idle', 'Status should be reset to idle'
		assert status['error_message'] is not None, 'Error message should describe the crash'
		assert status['error_stage'] == 'deadwood_segmentation'

	assert len(linear_calls) == 1, 'A Linear issue should be filed for the crash'
	assert linear_calls[0]['dataset_id'] == dataset_id
	assert notification_calls == [
		(crashed_dataset_task['task_id'], ProcessingNotificationType.failed)
	]


@pytest.fixture
def stale_active_task_without_stage_update(test_processor_user, auth_token):
	"""Create a task stuck with is_processing=True before any stage status was written."""
	task_id = None
	dataset_id = None
	try:
		with use_client(auth_token) as client:
			dataset_data = {
				'file_name': 'pre_stage_crash_test.tif',
				'user_id': test_processor_user,
				'license': 'CC BY',
				'platform': 'drone',
				'authors': ['Test Author'],
				'data_access': 'public',
				'aquisition_year': 2024,
				'aquisition_month': 1,
				'aquisition_day': 1,
			}
			dataset_response = client.table(settings.datasets_table).insert(dataset_data).execute()
			dataset_id = dataset_response.data[0]['id']

			status_data = {
				'dataset_id': dataset_id,
				'is_upload_done': True,
				'current_status': 'idle',
				'has_error': False,
			}
			client.table(settings.statuses_table).insert(status_data).execute()

			task_data = {
				'dataset_id': dataset_id,
				'user_id': test_processor_user,
				'task_types': [TaskTypeEnum.metadata],
				'priority': 1,
				'is_processing': True,
			}
			response = client.table(settings.queue_table).insert(task_data).execute()
			task_id = response.data[0]['id']

			yield {'task_id': task_id, 'dataset_id': dataset_id}

	finally:
		if auth_token:
			with use_client(auth_token) as client:
				if task_id:
					client.table(settings.queue_table).delete().eq('id', task_id).execute()
				if dataset_id:
					client.table(settings.statuses_table).delete().eq('dataset_id', dataset_id).execute()
					client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


def test_background_process_fails_stale_task_without_stage_update(
	stale_active_task_without_stage_update, auth_token, monkeypatch
):
	"""A stale active task whose requested stages are not complete is a hard-kill
	crash (no graceful shutdown cleaned it up), so it is failed and dequeued."""
	monkeypatch.setattr(processor_module, '_kill_dangling_dataset_resources', lambda dataset_id: None)
	monkeypatch.setattr(processor_module, 'create_processing_failure_issue', lambda **kwargs: None)

	dataset_id = stale_active_task_without_stage_update['dataset_id']

	background_process()

	with use_client(auth_token) as client:
		queue_response = (
			client.table(settings.queue_table).select('*')
			.eq('id', stale_active_task_without_stage_update['task_id']).execute()
		)
		assert len(queue_response.data) == 0, 'Crashed task should be removed from queue (no retry)'

		status_response = (
			client.table(settings.statuses_table).select('*')
			.eq('dataset_id', dataset_id).execute()
		)
		assert len(status_response.data) == 1
		status = status_response.data[0]
		assert status['has_error'] is True, 'Crashed task should be marked as error'
		assert status['current_status'] == 'idle'
		assert status['error_message'] is not None


@pytest.fixture
def stale_completed_active_task(test_processor_user, auth_token):
	"""Create a completed task that still has a stale active queue row."""
	task_id = None
	dataset_id = None
	try:
		with use_client(auth_token) as client:
			dataset_data = {
				'file_name': 'completed_stale_active_test.tif',
				'user_id': test_processor_user,
				'license': 'CC BY',
				'platform': 'drone',
				'authors': ['Test Author'],
				'data_access': 'public',
				'aquisition_year': 2024,
				'aquisition_month': 1,
				'aquisition_day': 1,
			}
			dataset_response = client.table(settings.datasets_table).insert(dataset_data).execute()
			dataset_id = dataset_response.data[0]['id']

			status_data = {
				'dataset_id': dataset_id,
				'is_upload_done': True,
				'current_status': 'idle',
				'is_metadata_done': True,
				'has_error': False,
			}
			client.table(settings.statuses_table).insert(status_data).execute()

			task_data = {
				'dataset_id': dataset_id,
				'user_id': test_processor_user,
				'task_types': [TaskTypeEnum.metadata],
				'priority': 1,
				'is_processing': True,
			}
			response = client.table(settings.queue_table).insert(task_data).execute()
			task_id = response.data[0]['id']

			yield {'task_id': task_id, 'dataset_id': dataset_id}

	finally:
		if auth_token:
			with use_client(auth_token) as client:
				if task_id:
					client.table(settings.queue_table).delete().eq('id', task_id).execute()
				if dataset_id:
					client.table(settings.statuses_table).delete().eq('dataset_id', dataset_id).execute()
					client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


def test_background_process_removes_stale_completed_active_task_without_marking_error(
	stale_completed_active_task, auth_token, monkeypatch
):
	"""Completed tasks should not be reclassified as crashes during stale queue recovery."""
	monkeypatch.setattr(processor_module, '_kill_dangling_dataset_resources', lambda dataset_id: None)
	notification_calls = []
	monkeypatch.setattr(
		processor_module,
		'_notify_processing_result_safely',
		lambda task, event_type, token: notification_calls.append((task.id, event_type)),
	)

	dataset_id = stale_completed_active_task['dataset_id']

	background_process()

	with use_client(auth_token) as client:
		queue_response = (
			client.table(settings.queue_table).select('*')
			.eq('id', stale_completed_active_task['task_id']).execute()
		)
		assert len(queue_response.data) == 0, 'Completed stale task should be removed from queue'

		status_response = (
			client.table(settings.statuses_table).select('*')
			.eq('dataset_id', dataset_id).execute()
		)
		assert len(status_response.data) == 1
		status = status_response.data[0]
		assert status['has_error'] is False, 'Completed stale task should not be marked as errored'
		assert status['current_status'] == 'idle', 'Completed status should remain idle'

	assert notification_calls == [
		(stale_completed_active_task['task_id'], ProcessingNotificationType.completed)
	]


def test_kill_dangling_resources_called_on_stale_task_recovery(crashed_dataset_task, monkeypatch):
	"""_kill_dangling_dataset_resources is called with the correct dataset_id when
	a stale active task is found, so orphaned containers and volumes are cleaned up."""
	killed_ids = []
	monkeypatch.setattr(processor_module, '_kill_dangling_dataset_resources', lambda dataset_id: killed_ids.append(dataset_id))
	monkeypatch.setattr(processor_module, 'create_processing_failure_issue', lambda **kwargs: None)

	background_process()

	assert crashed_dataset_task['dataset_id'] in killed_ids, (
		'_kill_dangling_dataset_resources should be called with the stale task dataset_id'
	)


def test_graceful_shutdown_requeues_inflight_task(crashed_dataset_task, auth_token):
	"""On an orderly shutdown (SIGTERM/SIGINT) the in-flight task is cleanly
	re-queued for retry — left in the queue, is_processing reset, status idle and
	no error — so deploys/restarts never lose or wrongly fail a healthy job."""
	import signal

	dataset_id = crashed_dataset_task['dataset_id']
	task_id = crashed_dataset_task['task_id']

	with use_client(auth_token) as client:
		row = client.table(settings.queue_table).select('*').eq('id', task_id).execute().data[0]

	task = QueueTask(
		id=row['id'],
		dataset_id=row['dataset_id'],
		user_id=row['user_id'],
		priority=row['priority'],
		is_processing=row['is_processing'],
		current_position=-1,
		estimated_time=None,
		task_types=row['task_types'],
	)

	processor_module._set_inflight_task(task)
	try:
		with pytest.raises(SystemExit) as exc_info:
			processor_module._handle_graceful_shutdown(signal.SIGTERM, None)
		assert exc_info.value.code == 0
	finally:
		processor_module._set_inflight_task(None)

	with use_client(auth_token) as client:
		queue_response = client.table(settings.queue_table).select('*').eq('id', task_id).execute()
		assert len(queue_response.data) == 1, 'Gracefully interrupted task should stay in queue for retry'
		assert queue_response.data[0]['is_processing'] is False, 'is_processing should be reset for re-pickup'

		status = client.table(settings.statuses_table).select('*').eq('dataset_id', dataset_id).execute().data[0]
		assert status['has_error'] is False, 'Graceful interruption is not an error'
		assert status['current_status'] == 'idle', 'Status should be reset to idle'
		assert status['error_message'] is None
		assert status['error_stage'] is None
