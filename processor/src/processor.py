import shutil
import signal
import sys
import docker
import socket
from pathlib import Path
from processor.src.process_geotiff import process_geotiff
from processor.src.process_odm import process_odm
from shared.models import QueueTask, TaskTypeEnum, StatusEnum
from shared.settings import settings
from shared.db import use_client, login, login_verified, verify_token
from shared.status import update_status
from shared.processing_tasks import downstream_tasks_missing_geotiff, format_missing_geotiff_error
from .processing_notifications import (
	ProcessingNotificationType,
	notify_processing_result_safely as _notify_processing_result_safely,
	reconcile_processing_notifications_safely as _reconcile_processing_notifications_safely,
)
from .process_thumbnail import process_thumbnail
from .process_cog import process_cog
from .process_deadwood_segmentation import process_deadwood_segmentation
from .process_treecover_segmentation import process_treecover_segmentation
from .process_deadwood_treecover_combined_v2 import process_deadwood_treecover_combined_v2
from .process_aoi_segmentation import process_aoi_segmentation
from .process_embeddings import process_embeddings
from .process_metadata import process_metadata
from .exceptions import AuthenticationError, ProcessingError
from .utils.linear_issues import create_processing_failure_issue
from .utils.drain_control import (
	BackgroundProcessResult,
	acknowledge_drain_request,
	clear_loop_unhealthy,
	is_drain_requested,
)
from .utils.queue_runtime import (
	claim_task,
	delete_queue_task,
	get_active_task,
	get_next_task,
	release_queue_task,
)
from shared.logging import LogContext, LogCategory, UnifiedLogger, SupabaseHandler

# Initialize logger with proper cleanup
logger = UnifiedLogger(__name__)


def _kill_dangling_dataset_resources(dataset_id: int):
	"""Kill any running containers and remove volumes left over from an interrupted job."""
	try:
		client = docker.from_env(timeout=settings.DOCKER_CLIENT_TIMEOUT_SECONDS)
		label_filter = f'dt_dataset_id={dataset_id}'

		containers = client.containers.list(all=True, filters={'label': label_filter})
		for c in containers:
			try:
				if c.status == 'running':
					c.stop(timeout=10)
				c.remove(force=True)
				logger.info(f'Removed dangling container {c.short_id} for dataset {dataset_id}')
			except Exception as e:
				logger.warning(f'Failed to remove dangling container {c.short_id} for dataset {dataset_id}: {e}')

		volumes = client.volumes.list(filters={'label': label_filter})
		for v in volumes:
			try:
				v.remove()
				logger.info(f'Removed dangling volume {v.name} for dataset {dataset_id}')
			except Exception as e:
				logger.warning(f'Failed to remove dangling volume {v.name} for dataset {dataset_id}: {e}')
	except Exception as e:
		logger.warning(f'Error during dangling resource cleanup for dataset {dataset_id}: {e}')
logger.add_supabase_handler(SupabaseHandler())

# Tracks the task currently being processed so the graceful-shutdown handler can
# cleanly return it to the queue when the container is asked to stop (deploy /
# restart). Holds the QueueTask while a stage is running; None when idle.
_inflight_task: QueueTask | None = None


def _set_inflight_task(task: QueueTask | None) -> None:
	global _inflight_task
	_inflight_task = task


def _handle_graceful_shutdown(signum, frame):
	"""Cleanly re-queue the in-flight task on an orderly shutdown (SIGTERM/SIGINT).

	This is what distinguishes a *transient* interruption from a *fault*. A deploy
	or manual restart sends SIGTERM, so this handler runs and returns the in-flight
	task to the waiting queue (is_processing=False, status idle) — it is retried
	transparently and leaves no stale active row behind. An OOM kill or hard crash
	delivers SIGKILL (or kills the process outright), so this handler never runs;
	the task is left as a stale is_processing=True row, which the crash-recovery
	path then treats as a genuine, non-retryable failure. Retrying OOM/bug crashes
	just loops and burns hours of compute, so we deliberately do not.
	"""
	task = _inflight_task
	if task is not None:
		try:
			# The token captured at task start may have expired during a long
			# stage; log in fresh for the bookkeeping writes.
			token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)
			logger.warning(
				f'Received signal {signum}; gracefully re-queuing in-flight task {task.id} '
				f'for dataset {task.dataset_id} for retry',
				LogContext(
					category=LogCategory.PROCESS,
					dataset_id=task.dataset_id,
					user_id=task.user_id,
					token=token,
				),
			)
			# ODM/TCD stages run as detached containers via the host Docker socket
			# and outlive this process. Kill them before making the task retryable,
			# otherwise the next run starts a duplicate while the old one keeps
			# consuming GPU/CPU.
			_kill_dangling_dataset_resources(task.dataset_id)
			with use_client(token) as client:
				client.table(settings.statuses_table).update({
					'current_status': StatusEnum.idle.value,
					'has_error': False,
					'error_message': None,
					'error_stage': None,
				}).eq('dataset_id', task.dataset_id).execute()
			release_queue_task(token, task)
		except Exception as e:
			# Best-effort: if we cannot clean up, fall through and exit anyway. The
			# task is then left as a stale active row and treated as a crash next run.
			logger.error(f'Failed to gracefully re-queue task {task.id} during shutdown: {e}')
	sys.exit(0)

# Maps each task type to its corresponding is_*_done flag and human-readable stage name.
# Used by crash detection to determine exactly which stage a previous run crashed during.
PIPELINE_STAGE_MAP = [
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


def _stage_done_flags(done_flags: str | tuple[str, ...]) -> tuple[str, ...]:
	return (done_flags,) if isinstance(done_flags, str) else done_flags


def refresh_processor_token(task: QueueTask, fallback_token: str | None = None) -> str:
	"""Best-effort token refresh for stage-boundary logging and updates."""
	try:
		return login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)
	except Exception:
		if fallback_token is not None:
			return fallback_token
		raise AuthenticationError('Invalid processor token', token=fallback_token, task_id=task.id)


def get_worker_id() -> str:
	"""Return a stable processor identity for queue ownership."""
	if settings.PROCESSOR_WORKER_ID:
		return settings.PROCESSOR_WORKER_ID
	if settings.DEV_MODE:
		return f'local-dev-{socket.gethostname()}'
	for machine_id_path in (Path('/host/etc/machine-id'), Path('/etc/machine-id')):
		try:
			machine_id = machine_id_path.read_text().strip()
		except OSError:
			continue
		if machine_id:
			return f'host-{machine_id[:12]}'
	raise RuntimeError('PROCESSOR_WORKER_ID must be set to a unique stable value for this processor host')


def detect_crashed_stage(status_data: dict, task_types: list) -> str:
	"""Determine which pipeline stage a previous crash occurred during.

	Walks the pipeline in order and returns the first stage that was requested
	but not yet marked as done in v2_statuses.

	Args:
		status_data: Row from v2_statuses table
		task_types: List of TaskTypeEnum values from the queue task

	Returns:
		str: Human-readable stage name where the crash occurred
	"""
	for task_type, done_flags, stage_name in PIPELINE_STAGE_MAP:
		if task_type in task_types and not all(status_data.get(flag, False) for flag in _stage_done_flags(done_flags)):
			return stage_name
	return 'unknown'


def get_completed_stages(status_data: dict) -> list[str]:
	"""Get list of pipeline stages that completed successfully before the crash.

	Args:
		status_data: Row from v2_statuses table

	Returns:
		list[str]: Human-readable names of completed stages
	"""
	completed = []
	for task_type, done_flags, stage_name in PIPELINE_STAGE_MAP:
		if all(status_data.get(flag, False) for flag in _stage_done_flags(done_flags)):
			completed.append(stage_name)
	return completed


def are_requested_stages_complete(status_data: dict, task_types: list) -> bool:
	"""Return True when all requested pipeline stages are already marked complete."""
	requested = [
		flag
		for task_type, done_flags, _ in PIPELINE_STAGE_MAP
		if task_type in task_types
		for flag in _stage_done_flags(done_flags)
	]
	return bool(requested) and all(status_data.get(done_flag, False) for done_flag in requested)


def is_dataset_uploaded_or_processed(task: QueueTask, token: str) -> tuple:
	"""Check if a dataset is ready for processing by verifying its upload status and error status.

	Args:
	    task (QueueTask): The task to check
	    token (str): Authentication token

	Returns:
	    tuple: (is_ready: bool, has_error: bool)
	        - is_ready: True if dataset is uploaded and ready for processing
	        - has_error: True if dataset has errors (should be removed from queue)
	"""
	with use_client(token) as client:
		response = (
			client.table(settings.statuses_table)
			.select('is_upload_done,has_error')
			.eq('dataset_id', task.dataset_id)
			.execute()
		)

		if not response.data:
			logger.warning(
				f'No status found for dataset {task.dataset_id}', extra={'token': token, 'dataset_id': task.dataset_id}
			)
			return False, False

		is_uploaded = response.data[0]['is_upload_done']
		has_error = response.data[0].get('has_error', False)  # Default to False if field doesn't exist

		if has_error:
			logger.warning(
				f'Dataset {task.dataset_id} has errors, will remove from queue',
				extra={'token': token, 'dataset_id': task.dataset_id},
			)
			return False, True

		msg = f'dataset upload status: {is_uploaded}'
		logger.info(msg, extra={'token': token})

		return is_uploaded, False


def _fail_crashed_task(token: str, task: QueueTask, status: dict | None) -> None:
	"""Mark a crashed task's dataset as errored, file a Linear issue, and dequeue it.

	Used when a previous run died mid-stage without a graceful shutdown (SIGKILL /
	OOM / hard crash). These failures are deterministic — retrying just loops and
	wastes hours of compute — so the task is failed for human attention rather than
	re-queued. Shared by both crash-recovery paths so they stay consistent.
	"""
	if status is not None:
		crashed_stage = detect_crashed_stage(status, task.task_types)
		completed = get_completed_stages(status)
		error_msg = f'Processing container crashed during {crashed_stage}. Completed: {completed}'
	else:
		crashed_stage = 'unknown'
		error_msg = 'Processing container crashed before a status row was written'

	logger.warning(
		f'Crash detected for dataset {task.dataset_id}: {error_msg}',
		LogContext(category=LogCategory.PROCESS, dataset_id=task.dataset_id, user_id=task.user_id, token=token),
	)

	# Mark as errored and reset to idle so it is no longer seen as in-progress.
	update_status(
		token,
		dataset_id=task.dataset_id,
		current_status=StatusEnum.idle,
		has_error=True,
		error_message=error_msg,
		error_stage=crashed_stage if crashed_stage != 'unknown' else None,
	)

	# Create Linear issue for visibility
	try:
		create_processing_failure_issue(
			token=token,
			dataset_id=task.dataset_id,
			stage=crashed_stage,
			error_message=error_msg,
		)
	except Exception as linear_error:
		logger.warning(f'Failed to create Linear issue for crash: {linear_error}')

	_notify_processing_result_safely(task, ProcessingNotificationType.failed, token)

	# Remove from queue — the dataset must be explicitly re-triggered once fixed.
	delete_queue_task(token, task)


def process_task(task: QueueTask, token: str):
	# Verify token
	user = verify_token(token)
	if not user:
		logger.error(
			'Invalid token for processing',
			LogContext(category=LogCategory.AUTH, dataset_id=task.dataset_id, user_id=task.user_id, token=token),
		)
		raise AuthenticationError('Invalid token', token=token, task_id=task.id)

	# Log start of processing
	logger.info(
		f'Starting processing for task {task.id}',
		LogContext(
			category=LogCategory.PROCESS,
			dataset_id=task.dataset_id,
			user_id=task.user_id,
			token=token,
			extra={'task_types': [t.value for t in task.task_types]},
		),
	)

	# Register as in-flight after the queue row has been claimed so a SIGTERM can
	# release this worker's task without touching another worker's active row.
	_set_inflight_task(task)
	# remove processing path if it exists
	if Path(settings.processing_path).exists():
		shutil.rmtree(settings.processing_path, ignore_errors=True)

	try:
		downstream_without_geotiff = downstream_tasks_missing_geotiff(task.task_types)
		if downstream_without_geotiff:
			error_message = format_missing_geotiff_error(downstream_without_geotiff)
			raise ProcessingError(
				error_message,
				task_type='geotiff_dependency',
				task_id=task.id,
				dataset_id=task.dataset_id,
			)

		# Process ODM first if it's in the list (generates orthomosaic for ZIP uploads)
		if TaskTypeEnum.odm_processing in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					'Starting ODM processing',
					LogContext(category=LogCategory.ODM, dataset_id=task.dataset_id, user_id=task.user_id, token=token),
				)
				process_odm(task, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'ODM processing failed: {str(e)}',
					LogContext(
						category=LogCategory.ODM,
						dataset_id=task.dataset_id,
						user_id=task.user_id,
						token=token,
						extra={'error': str(e)},
					),
				)
				raise ProcessingError(str(e), task_type='odm_processing', task_id=task.id, dataset_id=task.dataset_id)

		# Process convert_geotiff if it's in the list (handles ortho creation for both upload types)
		if TaskTypeEnum.geotiff in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					'Starting GeoTIFF conversion',
					LogContext(
						category=LogCategory.ORTHO, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				process_geotiff(task, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'GeoTIFF conversion failed: {str(e)}',
					LogContext(
						category=LogCategory.ORTHO,
						dataset_id=task.dataset_id,
						user_id=task.user_id,
						token=token,
						extra={'error': str(e)},
					),
				)
				raise ProcessingError(str(e), task_type='geotiff', task_id=task.id, dataset_id=task.dataset_id)

		# Process metadata if requested
		if TaskTypeEnum.metadata in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					'processing metadata',
					LogContext(
						category=LogCategory.METADATA, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				process_metadata(task, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'Metadata processing failed: {str(e)}',
					LogContext(
						category=LogCategory.METADATA, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				raise ProcessingError(str(e), task_type='metadata', task_id=task.id, dataset_id=task.dataset_id)

		# Process cog if requested
		if TaskTypeEnum.cog in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					f'processing cog to {settings.processing_path}',
					LogContext(category=LogCategory.COG, dataset_id=task.dataset_id, user_id=task.user_id, token=token),
				)
				process_cog(task, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'COG processing failed: {str(e)}',
					LogContext(
						category=LogCategory.COG, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				raise ProcessingError(str(e), task_type='cog', task_id=task.id, dataset_id=task.dataset_id)

		# Process thumbnail if requested
		if TaskTypeEnum.thumbnail in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					f'processing thumbnail to {settings.processing_path}',
					LogContext(
						category=LogCategory.THUMBNAIL, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				process_thumbnail(task, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'Thumbnail processing failed: {str(e)}',
					LogContext(
						category=LogCategory.THUMBNAIL,
						dataset_id=task.dataset_id,
						user_id=task.user_id,
						token=token,
					),
				)
				raise ProcessingError(str(e), task_type='thumbnail', task_id=task.id, dataset_id=task.dataset_id)

		# Process deadwood_segmentation if requested
		if TaskTypeEnum.deadwood_v1 in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					'processing deadwood segmentation',
					LogContext(
						category=LogCategory.DEADWOOD, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				process_deadwood_segmentation(task, token, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'Deadwood segmentation failed: {str(e)}',
					LogContext(
						category=LogCategory.DEADWOOD,
						dataset_id=task.dataset_id,
						user_id=task.user_id,
						token=token,
					),
				)
				raise ProcessingError(
					str(e), task_type='deadwood_segmentation', task_id=task.id, dataset_id=task.dataset_id
				)

		# Process treecover_segmentation if requested (runs after deadwood)
		if TaskTypeEnum.treecover_v1 in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					'processing tree cover segmentation',
					LogContext(
						category=LogCategory.TREECOVER, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				process_treecover_segmentation(task, token, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'Tree cover segmentation failed: {str(e)}',
					LogContext(
						category=LogCategory.TREECOVER,
						dataset_id=task.dataset_id,
						user_id=task.user_id,
						token=token,
					),
				)
				raise ProcessingError(
					str(e), task_type='treecover_segmentation', task_id=task.id, dataset_id=task.dataset_id
				)

		# Process combined deadwood+treecover segmentation if requested
		if TaskTypeEnum.deadwood_treecover_combined_v2 in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					'processing combined deadwood+treecover segmentation',
					LogContext(
						category=LogCategory.DEADWOOD, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				process_deadwood_treecover_combined_v2(task, token, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'Combined segmentation failed: {str(e)}',
					LogContext(
						category=LogCategory.DEADWOOD,
						dataset_id=task.dataset_id,
						user_id=task.user_id,
						token=token,
					),
				)
				raise ProcessingError(
					str(e),
					task_type='deadwood_treecover_combined_segmentation',
					task_id=task.id,
					dataset_id=task.dataset_id,
				)

		# Generate the automatic AOI polygon if requested (runs before audit)
		if TaskTypeEnum.aoi_v1 in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					'processing AOI segmentation',
					LogContext(category=LogCategory.AOI, dataset_id=task.dataset_id, user_id=task.user_id, token=token),
				)
				process_aoi_segmentation(task, token, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'AOI segmentation failed: {str(e)}',
					LogContext(
						category=LogCategory.AOI,
						dataset_id=task.dataset_id,
						user_id=task.user_id,
						token=token,
					),
				)
				raise ProcessingError(str(e), task_type='aoi_segmentation', task_id=task.id, dataset_id=task.dataset_id)

		# Compute per-tile CLIP embeddings for open-vocabulary search.
		# Runs last so the standardized ortho and all prior stages are already done.
		if TaskTypeEnum.embeddings_v1 in task.task_types:
			try:
				token = refresh_processor_token(task, token)
				logger.info(
					'processing tile embeddings',
					LogContext(
						category=LogCategory.EMBEDDINGS, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				process_embeddings(task, token, settings.processing_path)
			except Exception as e:
				token = refresh_processor_token(task, token)
				logger.error(
					f'Tile embedding failed: {str(e)}',
					LogContext(
						category=LogCategory.EMBEDDINGS,
						dataset_id=task.dataset_id,
						user_id=task.user_id,
						token=token,
					),
				)
				raise ProcessingError(str(e), task_type='embedding_processing', task_id=task.id, dataset_id=task.dataset_id)

	except Exception as e:
		# This path owns the failure bookkeeping; clear the in-flight marker now so a
		# SIGTERM mid-handling cannot re-queue the failed task or clear its error.
		_set_inflight_task(None)

		logger.error(
			f'Processing failed: {str(e)}',
			LogContext(category=LogCategory.PROCESS, dataset_id=task.dataset_id, user_id=task.user_id, token=token),
		)
		update_status(
			token,
			dataset_id=task.dataset_id,
			current_status=StatusEnum.idle,
			has_error=True,
			error_message=str(e),
			error_stage=e.task_type if isinstance(e, ProcessingError) else 'processing',
		)

		try:
			stage = e.task_type if isinstance(e, ProcessingError) else 'processing'
			create_processing_failure_issue(
				token=token,
				dataset_id=task.dataset_id,
				stage=stage,
				error_message=str(e),
			)
		except Exception as linear_error:
			logger.warning(f'Failed to create Linear issue: {linear_error}')

		_notify_processing_result_safely(task, ProcessingNotificationType.failed, token)

		try:
			delete_token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)
			delete_queue_task(delete_token, task)
			logger.info(
				f'Removed failed task {task.id} from queue',
				LogContext(category=LogCategory.PROCESS, dataset_id=task.dataset_id, user_id=task.user_id, token=token),
			)
		except Exception as delete_error:
			logger.error(
				f'Failed to remove task {task.id} from queue: {delete_error}',
				LogContext(category=LogCategory.PROCESS, dataset_id=task.dataset_id, user_id=task.user_id, token=token),
			)
		raise

	else:
		token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)
		# All processing stages are complete. Keep the queue row claimed so a stop
		# during notification I/O is recovered as completed work, not reprocessed.
		_set_inflight_task(None)
		_notify_processing_result_safely(task, ProcessingNotificationType.completed, token)
		delete_queue_task(token, task)

	finally:
		_set_inflight_task(None)

		if not settings.DEV_MODE:
			shutil.rmtree(settings.processing_path, ignore_errors=True)


def background_process() -> BackgroundProcessResult:
	"""
	Process the next healthy task from the queue, if there is one.

	Returns WORKED when this worker claimed and completed a task, FAILED when it
	claimed a task but the processing path raised after bookkeeping, and IDLE
	when the queue has no ready work or the host requested a drain.

	On each call this function:
	1. Logs in as the processor service account.
	2. Installs a graceful-shutdown handler so a deploy/restart (SIGTERM) cleanly
	   re-queues the in-flight task for retry instead of leaving it stranded.
	3. Handles this worker's stale `is_processing=true` queue row left behind by
	   a previous run. Because graceful stops self-clean via the shutdown handler,
	   a leftover active row can only mean a hard kill (SIGKILL/OOM) or hard crash:
	   - If all requested stages are already done, the row is just removed.
	   - Otherwise it is a genuine, non-retryable crash: the dataset is marked
	     errored, a Linear issue is filed, and the task is removed from the queue.
	     OOM/bug crashes are deterministic, so retrying only loops and wastes
	     compute — we deliberately do not retry them.
	4. Detects crashes the same way for the next waiting task, then processes the
	   first healthy, ready task and returns.

	Multiple workers can read the same waiting row, but only one can atomically
	claim it by flipping `is_processing=false` to true and recording `claimed_by`.
	"""
	# Install graceful-shutdown handlers so deploys/restarts re-queue cleanly.
	signal.signal(signal.SIGTERM, _handle_graceful_shutdown)
	signal.signal(signal.SIGINT, _handle_graceful_shutdown)

	token, user = login_verified(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)
	if not user:
		raise Exception(status_code=401, detail='Invalid token after fresh login')

	worker_id = get_worker_id()
	active_recovery_attempted = False

	while True:
		active_task = None if active_recovery_attempted else get_active_task(token, worker_id)
		# A successful active-queue poll proves the restarted worker can reach the
		# queue. Clear any persisted crash-loop marker before recovery or processing
		# can become long-running, so host maintenance does not stop healthy work.
		clear_loop_unhealthy()
		active_recovery_attempted = True
		if active_task is not None:
			logger.warning(
				f'Found stale active queue task {active_task.id} for dataset {active_task.dataset_id}; '
				'previous run died without graceful shutdown',
				LogContext(
					category=LogCategory.PROCESS,
					dataset_id=active_task.dataset_id,
					user_id=active_task.user_id,
					token=token,
				),
			)

			with use_client(token) as client:
				status_resp = client.table(settings.statuses_table) \
					.select('*').eq('dataset_id', active_task.dataset_id).execute()

			_kill_dangling_dataset_resources(active_task.dataset_id)

			status = status_resp.data[0] if status_resp.data else None

			if status is not None and status.get('has_error'):
				try:
					_notify_processing_result_safely(active_task, ProcessingNotificationType.failed, token)
				except Exception as notification_error:
					logger.warning(f'Keeping failed task {active_task.id} until notification persistence recovers: {notification_error}')
					continue
				delete_queue_task(token, active_task)
				continue

			if status is not None and are_requested_stages_complete(status, active_task.task_types):
				# Crashed only after finishing all requested stages — no fault to
				# report, just remove the stale queue row.
				logger.info(
					f'Removing stale completed queue task {active_task.id} for dataset {active_task.dataset_id}',
					LogContext(
						category=LogCategory.PROCESS,
						dataset_id=active_task.dataset_id,
						user_id=active_task.user_id,
						token=token,
					),
				)
				update_status(
					token,
					dataset_id=active_task.dataset_id,
					current_status=StatusEnum.idle,
					has_error=False,
				)
				try:
					_notify_processing_result_safely(active_task, ProcessingNotificationType.completed, token)
				except Exception as notification_error:
					logger.warning(f'Keeping completed task {active_task.id} until notification persistence recovers: {notification_error}')
					continue
				delete_queue_task(token, active_task)
				continue

			# Genuine mid-stage crash — fail it instead of retrying.
			try:
				_fail_crashed_task(token, active_task, status)
			except Exception as finalization_error:
				logger.warning(f'Keeping failed task {active_task.id} until finalization recovers: {finalization_error}')
			continue

		if is_drain_requested():
			# Exercise the release's queue-read contract before advertising readiness.
			# This is read-only: no task is claimed while a drain is active.
			get_next_task(token)
			acknowledge_drain_request(worker_id)
			return BackgroundProcessResult.IDLE

		task = get_next_task(token)
		if task is None:
			print('No tasks in the queue.')
			_reconcile_processing_notifications_safely()
			return BackgroundProcessResult.IDLE

		if is_drain_requested():
			acknowledge_drain_request(worker_id)
			return BackgroundProcessResult.IDLE

		is_ready, has_error = is_dataset_uploaded_or_processed(task, token)

		if has_error:
			claimed_task = claim_task(token, task, worker_id)
			if claimed_task is None:
				logger.info(
					f'Skipping errored queue task {task.id}; another worker claimed it first',
					LogContext(
						category=LogCategory.PROCESS, dataset_id=task.dataset_id, user_id=task.user_id, token=token
					),
				)
				continue

			# Dataset already has errors - remove task from queue
			logger.info(
				f'Removing errored task {task.id} for dataset {task.dataset_id} from queue',
				LogContext(
					category=LogCategory.PROCESS, dataset_id=task.dataset_id, user_id=task.user_id, token=token
				),
			)
			delete_queue_task(token, claimed_task)
			continue

		if not is_ready:
			# Not uploaded yet - skip, try again on a later poll
			logger.info(
				f'Skipping task {task.id} - dataset not uploaded yet; will retry later',
				LogContext(
					category=LogCategory.PROCESS, dataset_id=task.dataset_id, user_id=task.user_id, token=token
				),
			)
			_reconcile_processing_notifications_safely()
			return BackgroundProcessResult.IDLE

		claimed_task = claim_task(token, task, worker_id)
		if claimed_task is None:
			logger.info(
				f'Skipping queue task {task.id}; another worker claimed it first',
				LogContext(
					category=LogCategory.PROCESS, dataset_id=task.dataset_id, user_id=task.user_id, token=token
				),
			)
			continue
		task = claimed_task

		if is_drain_requested():
			acknowledge_drain_request(worker_id)
			release_queue_task(token, task)
			return BackgroundProcessResult.IDLE

		# CRASH DETECTION: check if a previous run crashed mid-processing
		with use_client(token) as client:
			status_resp = client.table(settings.statuses_table) \
				.select('*').eq('dataset_id', task.dataset_id).execute()

		if status_resp.data:
			status = status_resp.data[0]
			if status['current_status'] != 'idle':
				# Previous crash detected - current_status is still set to a
				# processing stage. Fail it (no retry) for the same reason as the
				# stale-active-task path above.
				_fail_crashed_task(token, task, status)
				continue  # check next task in queue

		# Normal processing - found a healthy, ready task
		logger.info(
			f'Start processing queued task: {task}.',
			LogContext(
				category=LogCategory.PROCESS, dataset_id=task.dataset_id, user_id=task.user_id, token=token
			),
		)
		try:
			process_task(task, token=token)
		except Exception:
			_reconcile_processing_notifications_safely()
			return BackgroundProcessResult.FAILED
		_reconcile_processing_notifications_safely()
		return BackgroundProcessResult.WORKED


if __name__ == '__main__':
	background_process()
