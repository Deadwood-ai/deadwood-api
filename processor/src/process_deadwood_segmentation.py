from fastapi import HTTPException
from pathlib import Path

from ...shared.supabase import use_client, login, verify_token
from ...shared.settings import settings
from ...shared.models import StatusEnum, Dataset, QueueTask
from ...shared.logger import logger
from .utils import update_status, pull_file_from_storage_server
from .deadwood_segmentation.predict_deadwood import predict_deadwood


def process_deadwood_segmentation(task: QueueTask, token: str, temp_dir: Path):
	# login with the processor
	token = login(settings.processor_username, settings.processor_password)

	user = verify_token(token)
	if not user:
		return HTTPException(status_code=401, detail='Invalid token')

	try:
		with use_client(token) as client:
			response = client.table(settings.datasets_table).select('*').eq('id', task.dataset_id).execute()
			dataset = Dataset(**response.data[0])
	except Exception as e:
		logger.error(f'Error: {e}')
		return HTTPException(status_code=500, detail='Error fetching dataset')

	# update_status(token, dataset_id=dataset.id, status=StatusEnum.deadwood_prediction)
	update_status(token, dataset_id=dataset.id, status=StatusEnum.processing)

	# get local file path
	file_path = Path(temp_dir) / dataset.file_name
	# get the remote file path
	storage_server_file_path = f'{settings.storage_server_data_path}/archive/{dataset.file_name}'
	pull_file_from_storage_server(storage_server_file_path, str(file_path), token)

	try:
		logger.info(
			f'Running deadwood segmentation for dataset {task.dataset_id} with file path {str(file_path)} with command:',
			extra={'token': token},
		)
		predict_deadwood(task.dataset_id, file_path)
	except Exception as e:
		logger.error(f'Error: {e}', extra={'token': token})
		# update_status(token, dataset_id=dataset.id, status=StatusEnum.deadwood_errored)
		update_status(token, dataset_id=dataset.id, status=StatusEnum.errored)
		return HTTPException(status_code=500, detail='Error running deadwood segmentation')

	logger.info(f'Deadwood segmentation completed for dataset {task.dataset_id}', extra={'token': token})
	token = login(settings.processor_username, settings.processor_password)
	update_status(token, dataset_id=dataset.id, status=StatusEnum.processed)
