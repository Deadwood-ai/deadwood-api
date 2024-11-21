from pathlib import Path
import time
from fastapi import HTTPException

from ...shared.supabase import use_client, login, verify_token
from ...shared.settings import settings
from ...shared.models import StatusEnum, Dataset, QueueTask, Thumbnail
from ...shared.logger import logger
from .thumbnail.thumbnail import calculate_thumbnail
from .utils import update_status, pull_file_from_storage_server, push_file_to_storage_server


def process_thumbnail(task: QueueTask, temp_dir: Path):
	# login with the processor
	token = login(settings.processor_username, settings.processor_password)

	user = verify_token(token)
	if not user:
		return HTTPException(status_code=401, detail='Invalid token')

	# load the dataset
	try:
		with use_client(token) as client:
			response = client.table(settings.datasets_table).select('*').eq('id', task.dataset_id).execute()
			dataset = Dataset(**response.data[0])
	except Exception as e:
		logger.error(f'Error: {e}')
		return HTTPException(status_code=500, detail='Error fetching dataset')

	# update the status to processing
	update_status(token, dataset_id=dataset.id, status=StatusEnum.thumbnail_processing)

	# get local file paths
	input_path = Path(temp_dir) / dataset.file_name
	thumbnail_file_name = dataset.file_name.replace('.tif', '.jpg')
	output_path = Path(temp_dir) / thumbnail_file_name

	# get the remote file path
	storage_server_file_path = f'{settings.storage_server_data_path}/archive/{dataset.file_name}'
	pull_file_from_storage_server(storage_server_file_path, str(input_path), token)

	t1 = time.time()
	logger.info(f'Calculate Thumbnail for dataset {dataset.id}', extra={'token': token})
	calculate_thumbnail(str(input_path), str(output_path))
	logger.info(
		f'Thumbnail generated for dataset {dataset.id}',
		extra={'token': token},
	)

	# push the file to the storage server
	storage_server_thumbnail_path = f'{settings.storage_server_data_path}/thumbnails/{thumbnail_file_name}'
	push_file_to_storage_server(str(output_path), storage_server_thumbnail_path, token)

	t2 = time.time()

	# fill the metadata
	meta = dict(
		dataset_id=dataset.id,
		user_id=task.user_id,
		thumbnail_path=thumbnail_file_name,
		runtime=t2 - t1,
	)

	thumbnail = Thumbnail(**meta)

	# check if session is still active and token is valid
	token = login(settings.processor_username, settings.processor_password)
	user = verify_token(token)
	if not user:
		return HTTPException(status_code=401, detail='Invalid token')

	try:
		with use_client(token) as client:
			# Use upsert instead of delete and insert
			client.table(settings.thumbnail_table).upsert(
				thumbnail.model_dump(),
				on_conflict='dataset_id',  # Assuming dataset_id is the primary key
			).execute()
	except Exception as e:
		logger.error(f'Error: {e}')
		return HTTPException(status_code=500, detail='Error saving thumbnail to database')

	# If we reach here, processing was successful
	update_status(token, dataset.id, StatusEnum.processed)

	logger.info(
		f'Finished creating thumbnail for dataset {dataset.id}.',
		extra={'token': token},
	)
