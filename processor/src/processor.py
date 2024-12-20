import shutil
import time

from shared.models import QueueTask, StatusEnum, Dataset
from shared.settings import settings
from shared.supabase import use_client, login, verify_token
from shared.logger import logger
from .process_thumbnail import process_thumbnail
from .process_cog import process_cog
from .process_deadwood_segmentation import process_deadwood_segmentation
from .exceptions import ProcessorError, AuthenticationError, DatasetError, ProcessingError, StorageError


def current_running_tasks(token: str) -> int:
	"""Get the number of currently actively processing tasks from supabase.

	Args:
	    token (str): Client access token for supabase

	Returns:
	    int: number of currently active tasks
	"""
	with use_client(token) as client:
		response = client.table(settings.queue_table).select('id').eq('is_processing', True).execute()
		num_of_tasks = len(response.data)

	return num_of_tasks


def queue_length(token: str) -> int:
	"""Get the number of tasks in the queue from supabase.

	Args:
	    token (str): Client access token for supabase

	Returns:
	    int: number of all tasks in the queue
	"""
	with use_client(token) as client:
		response = client.table(settings.queue_position_table).select('id').execute()
		num_of_tasks = len(response.data)

	return num_of_tasks


def get_next_task(token: str) -> QueueTask:
	"""Get the next task (QueueTask class) in the queue from supabase.

	Args:
	    token (str): Client access token for supabase

	Returns:
	    QueueTask: The next task in the queue as a QueueTask class instance
	"""
	with use_client(token) as client:
		response = client.table(settings.queue_position_table).select('*').limit(1).execute()
	if not response.data or len(response.data) == 0:
		return None
	return QueueTask(**response.data[0])


def is_dataset_uploaded_or_processed(task: QueueTask, token: str) -> bool:
	with use_client(token) as client:
		response = client.table(settings.datasets_table).select('*').eq('id', task.dataset_id).execute()
		msg = f'dataset status: {response.data[0]["status"]}'
		logger.info(msg, extra={'token': token})
	if response.data[0]['status'] in [StatusEnum.uploaded, StatusEnum.processed]:
		return True
	return False


def process_task(task: QueueTask, token: str):
	try:
		# Verify token
		user = verify_token(token)
		if not user:
			raise AuthenticationError('Invalid token', token=token, task_id=task.id)

		# Load dataset
		try:
			with use_client(token) as client:
				response = client.table(settings.datasets_table).select('*').eq('id', task.dataset_id).execute()
				dataset = Dataset(**response.data[0])
		except Exception as e:
			raise DatasetError(f'Failed to fetch dataset: {str(e)}', dataset_id=task.dataset_id, task_id=task.id)

		# Process based on task type
		if task.task_type in ['cog', 'all']:
			try:
				logger.info(f'processing cog to {settings.processing_path}')
				process_cog(task, settings.processing_path)
			except Exception as e:
				raise ProcessingError(str(e), task_type='cog', task_id=task.id, dataset_id=task.dataset_id)

		if task.task_type in ['thumbnail', 'all']:
			try:
				logger.info(f'processing thumbnail to {settings.processing_path}')
				process_thumbnail(task, settings.processing_path)
			except Exception as e:
				raise ProcessingError(str(e), task_type='thumbnail', task_id=task.id, dataset_id=task.dataset_id)
		# if task.task_type in ['deadwood_segmentation', 'all']:
		# 	try:
		# 		process_deadwood_segmentation(task, token, settings.processing_path)
		# 	except Exception as e:
		# 		raise ProcessingError(
		# 			str(e), task_type='deadwood_segmentation', task_id=task.id, dataset_id=task.dataset_id
		# 		)

		# Delete task after successful processing
		try:
			with use_client(token) as client:
				client.table(settings.queue_table).delete().eq('id', task.id).execute()
		except Exception as e:
			raise ProcessorError(
				f'Failed to delete completed task: {str(e)}', task_type=task.task_type, task_id=task.id
			)

	except (AuthenticationError, DatasetError, ProcessingError, StorageError) as e:
		logger.error(
			str(e),
			extra={
				'token': token,
				'task_id': getattr(e, 'task_id', None),
				'dataset_id': getattr(e, 'dataset_id', None),
				'error_type': e.__class__.__name__,
			},
		)
		raise
	except Exception as e:
		msg = f'Unexpected error: {str(e)}'
		logger.error(msg, extra={'token': token, 'task_id': task.id})
		raise ProcessorError(msg, task_type=task.task_type, task_id=task.id) from e
	finally:
		if not settings.DEV_MODE:
			shutil.rmtree(settings.processing_path, ignore_errors=True)


def background_process():
	"""
	This process checks if there is anything to do in the queue.
	If so, it checks the currently running tasks against the maximum allowed tasks.
	If another task can be started, it will do so, if not, the background_process is
	added to the FastAPI background tasks with a configured delay.

	"""
	# use the processor to log in
	token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)
	user = verify_token(token)
	if not user:
		raise Exception(status_code=401, detail='Invalid token')

	# get the number of currently running tasks
	num_of_running = current_running_tasks(token)
	queued_tasks = queue_length(token)

	# is there is nothing in the queue, just stop the process and log
	if queued_tasks == 0:
		# logger.info('No tasks in the queue.', extra={'token': token})
		return

	# check if we can start another task
	if num_of_running < settings.CONCURRENT_TASKS:
		# get the next task
		task = get_next_task(token)
		is_uploaded = is_dataset_uploaded_or_processed(task, token)
		if task is not None and is_uploaded:
			logger.info(
				f'Start a new background process for queued task: {task}.',
				extra={
					'token': token,
					# 'user_id': task.user_id,
					'dataset_id': task.dataset_id,
				},
			)
			process_task(task, token=token)

			# add another background process with a short timeout
			# Timer(interval=1, function=background_process).start()
		else:
			# we expected a task here, but there was None
			logger.error(
				'Task was expected to be uploaded, but was not.',
				extra={'token': token},
			)
	else:
		# inform no spot available
		logger.debug(
			'No spot available for new task.',
			extra={'token': token},
		)
		return
		# restart this process after the configured delay
		# Timer(interval=settings.task_retry_delay, function=background_process).start()


def run_processor():
	"""
	Main processor loop that runs continuously in development mode,
	or once in production mode (for cron job execution)
	"""
	try:
		if settings.DEV_MODE:
			logger.info('Starting processor in development mode (continuous loop)...')
			while True:
				background_process()
				time.sleep(30)  # Development polling interval
		else:
			logger.info('Starting processor in production mode (single run)...')
			background_process()

	except Exception as e:
		logger.error(f'Critical processor error: {e}')
		if settings.DEV_MODE:
			time.sleep(5)  # Wait before retrying in dev mode
			run_processor()  # Restart the loop in dev mode
		else:
			raise  # In production, let the error propagate


if __name__ == '__main__':
	run_processor()
