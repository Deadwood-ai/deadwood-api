from pathlib import Path
import time
import uuid

from shared.supabase import use_client, login, verify_token
from shared.settings import settings
from shared.models import StatusEnum, Dataset, QueueTask
from shared.logger import logger
from .utils import update_status, pull_file_from_storage_server, push_file_to_storage_server
from .exceptions import AuthenticationError, DatasetError, ProcessingError
from .geotiff.geotiff import convert_geotiff, verify_geotiff
from shared.geotiff import create_geotiff_info_entry


def process_geotiff(task: QueueTask, temp_dir: Path):
	token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)

	user = verify_token(token)
	if not user:
		raise AuthenticationError('Invalid processor token', token=token, task_id=task.id)

	try:
		with use_client(token) as client:
			response = client.table(settings.datasets_table).select('*').eq('id', task.dataset_id).execute()
			dataset = Dataset(**response.data[0])
	except Exception as e:
		raise DatasetError(f'Failed to fetch dataset: {str(e)}', dataset_id=task.dataset_id, task_id=task.id)

	update_status(token, dataset_id=dataset.id, status=StatusEnum.converting)

	try:
		# Setup paths
		input_path = temp_dir / dataset.file_name
		temp_uuid = uuid.uuid4()
		converted_path = temp_dir / f'converted_{temp_uuid}.tif'

		# Get storage server path
		storage_server_path = f'{settings.STORAGE_SERVER_DATA_PATH}/archive/{dataset.file_name}'

		# Pull original file
		pull_file_from_storage_server(storage_server_path, str(input_path), token)

		# Start conversion
		t1 = time.time()
		if not convert_geotiff(str(input_path), str(converted_path), token):
			raise ProcessingError('Conversion failed', task_type='convert', task_id=task.id, dataset_id=dataset.id)

		# Verify converted file
		if not verify_geotiff(str(converted_path), token):
			raise ProcessingError(
				'Converted file verification failed', task_type='convert', task_id=task.id, dataset_id=dataset.id
			)

		# If verification successful, replace original file on storage server
		push_file_to_storage_server(str(converted_path), storage_server_path, token)
		t2 = time.time()

		# Update GeoTiff info using the existing function
		create_geotiff_info_entry(converted_path, dataset.id, token)

		# Log conversion time
		logger.info(
			f'GeoTIFF conversion completed in {t2 - t1:.2f} seconds', extra={'token': token, 'dataset_id': dataset.id}
		)
		# Replace locally pulled file with converted file
		input_path.unlink()
		converted_path.rename(input_path)

	except Exception as e:
		raise ProcessingError(str(e), task_type='convert', task_id=task.id, dataset_id=dataset.id)
	finally:
		if converted_path.exists():
			converted_path.unlink()

	# Update final status
	update_status(token, dataset.id, StatusEnum.processed)
	logger.info(f'Finished converting dataset {dataset.id}', extra={'token': token})
