import os

# os.environ['SSH_PRIVATE_KEY_PATH'] = '/home/jj1049/.ssh/id_rsa'

from pathlib import Path
import requests
import time
import httpx
from tqdm import tqdm
import uuid
import fire
from typing import List

from shared.supabase import login, verify_token
from shared.settings import settings
from shared.models import MetadataPayloadData
from shared.logger import logger


def chunked_upload(file_path: Path, token: str, user_id: str, chunk_size: int = 100 * 1024 * 1024):
	"""
	Upload a file in chunks to the API endpoint with progress bar.

	Args:
		file_path: Path to the file to upload
		token: Authentication token
		user_id: ID of the user uploading the file
		chunk_size: Size of chunks in bytes (default 4MB)
	"""
	file_path = Path(file_path)
	file_size = file_path.stat().st_size
	chunks_total = (file_size + chunk_size - 1) // chunk_size
	upload_id = str(uuid.uuid4())
	start_time = time.time()

	# Configure progress bar
	progress = tqdm(total=file_size, unit='B', unit_scale=True, desc=f'Uploading {file_path.name}')
	logger.info(f'Uploading file: {file_path}, to endpoint: {settings.API_ENDPOINT}')

	with open(file_path, 'rb') as f, httpx.Client(timeout=httpx.Timeout(timeout=300.0)) as client:  # 5 min timeout
		for chunk_index in range(chunks_total):
			chunk_data = f.read(chunk_size)

			files = {'file': (file_path.name, chunk_data, 'application/octet-stream')}
			data = {
				'chunk_index': str(chunk_index),
				'chunks_total': str(chunks_total),
				'filename': file_path.name,
				'copy_time': str(int(time.time() - start_time)),
				'upload_id': upload_id,
			}

			try:
				response = client.post(
					f'{settings.API_ENDPOINT}/datasets/chunk',
					files=files,
					data=data,
					headers={'Authorization': f'Bearer {token}'},
				)
				response.raise_for_status()

				# Update progress bar
				progress.update(len(chunk_data))

				# If this is the final chunk, return the dataset information
				if chunk_index == chunks_total - 1:
					progress.close()
					return response.json()

			except httpx.RequestError as e:
				progress.close()
				logger.error(f'Error uploading chunk {chunk_index}: {e}')
				raise

			except Exception as e:
				progress.close()
				logger.error(f'Unexpected error during upload: {e}')
				raise

	progress.close()
	raise ValueError('Upload completed but no dataset was returned')


def update_metadata(dataset_id: str, metadata: MetadataPayloadData, token: str):
	try:
		res = requests.put(
			f'{settings.API_ENDPOINT}/datasets/{dataset_id}/metadata',
			json=metadata.model_dump(),
			headers={'Authorization': f'Bearer {token}'},
		)
		logger.info(f'response: {res.json()}')
		res.raise_for_status()
	except requests.exceptions.RequestException as e:
		logger.error(f'Error updating metadata: {e}')


def start_processing(dataset_id: str, token: str, task_type: str = 'all'):
	try:
		res = requests.put(
			f'{settings.API_ENDPOINT}/datasets/{dataset_id}/process',
			params={'task_type': task_type},
			headers={'Authorization': f'Bearer {token}'},
		)
		logger.info(f'response: {res.json()}')
		res.raise_for_status()
	except Exception as e:
		logger.error(f'Error updating process: {e}')


def import_data(
	file_path: str,
	aquisition_year: str,
	aquisition_month: str,
	aquisition_day: str,
	authors: str,
	data_access: str,
	platform: str,
	doi: str,
	additional_information: str,
):
	"""
	Upload a single file to the storage server and update the metadata and process status.
	"""
	token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)

	# Verify token and get user
	user = verify_token(token)
	if not user:
		raise ValueError('Invalid token')

	# Upload the file
	logger.info(f'Uploading file: {file_path}')
	dataset = chunked_upload(
		Path(file_path),
		token=token,
		user_id=user.id,  # Pass the verified user's ID
	)
	logger.info(f'Dataset: {dataset}')
	time.sleep(1)
	metadata = MetadataPayloadData(
		name=dataset['file_alias'],
		authors=authors,
		data_access=data_access,
		platform=platform,
		aquisition_year=aquisition_year,
		aquisition_month=aquisition_month,
		aquisition_day=aquisition_day,
		doi=doi,
		additional_information=additional_information,
	)
	logger.info(f'Adding metadata: {metadata}')
	update_metadata(dataset['id'], metadata, token)
	time.sleep(1)
	logger.info(f'Starting processing for dataset: {dataset["id"]}')
	start_processing(dataset['id'], token)


if __name__ == '__main__':
	fire.Fire(import_data)


# python import_data.py \
#     --file-path='/Users/januschvajna-jehle/data/deadwood-example-data/upload-test.tif' \
#     --aquisition-year="2024" \
#     --aquisition-month="11" \
#     --aquisition-day="19" \
#     --authors="Janusch Vajna-Jehle" \
#     --data-access="public" \
#     --platform="drone" \
#     --doi="10.5281/zenodo.12345678" \
#     --additional-information="This is a test"
