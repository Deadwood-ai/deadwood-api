from pathlib import Path
from typing import Optional
import time
import uuid

from ..shared.models import Dataset
from ..api.src.upload_service import UploadService
from ..shared.settings import settings
from ..shared.logger import logger
from ..processing import push_file_to_storage_server
from ..shared.models import MetadataPayloadData


def manual_upload(file_path: Path, token: str, user_id: Optional[str] = None) -> Dataset:
	"""
	Manually upload a file to the storage server and create database entries

	Args:
	    file_path: Path to the file to upload
	    token: Authentication token
	    user_id: User ID to assign to the dataset
	Returns:
	    Dataset: The created dataset object
	"""
	upload_service = UploadService(token)

	# Generate unique filename
	uid = str(uuid.uuid4())
	new_filename = f'{uid}_{Path(file_path).stem}.tif'
	# target_path = settings.archive_path / new_filename

	# Push file to storage server using the existing mechanism
	# logger.info(f'Pushing file to storage: {target_path}', extra={'token': token})
	t1 = time.time()
	remote_path = f'{settings.storage_server_data_path}/archive/{new_filename}'
	print(f'Pushing file to storage: {remote_path}')
	print(f'File path: {str(file_path)}')
	print(f'File Name: {new_filename}')
	push_file_to_storage_server(str(file_path), remote_path, token)
	t2 = time.time()
	copy_time = t2 - t1
	# Create dataset entry with all available information
	logger.info('Creating dataset entry...', extra={'token': token})

	dataset = upload_service.create_dataset_entry(
		file_path=file_path,
		new_file_name=new_filename,
		file_alias=file_path.name,
		user_id=user_id,
		copy_time=copy_time,
		manual_upload=True,
	)
	# Compute file information
	logger.info(f'Upload complete. Dataset ID: {dataset.id}', extra={'token': token, 'dataset_id': dataset.id})
	# adding metadata

	return dataset
