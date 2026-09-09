"""Simplified ZIP upload processing - only stores ZIP file, no extraction"""

from pathlib import Path
from shared.models import Dataset
from shared.status import update_status
from shared.models import StatusEnum
from shared.settings import settings
from shared.db import use_client
from shared.zip_utils import ensure_supported_zip_compression


def process_raw_images_upload(dataset: Dataset, upload_target_path: Path, token: str) -> Dataset:
	"""Process ZIP upload with minimal logic - only store ZIP file, no extraction or analysis

	Args:
		dataset: The dataset object created during upload
		upload_target_path: Path to the uploaded ZIP file
		token: Authentication token for database operations

	Returns:
		Dataset: The updated dataset object

	Note:
		This function only handles ZIP file storage. All extraction, RTK detection, and
		technical analysis is deferred to the ODM processing task.
	"""
	# Defensive validation in case this function is called outside the
	# normal upload endpoint flow.
	ensure_supported_zip_compression(upload_target_path)

	# Move ZIP file to expected location for ODM processor
	zip_filename = f'{dataset.id}.zip'
	final_zip_path = settings.raw_images_path / zip_filename
	upload_target_path.rename(final_zip_path)

	# Get basic ZIP file info for database entry
	zip_size_bytes = final_zip_path.stat().st_size
	zip_size_mb = max(1, zip_size_bytes // (1024 * 1024))  # Convert to MB, minimum 1

	# Create raw_images database entry with minimal info
	# RTK metadata and image count will be populated during ODM processing
	raw_images_data = {
		'dataset_id': dataset.id,
		'raw_image_count': 0,  # Will be updated during ODM processing
		'raw_image_size_mb': zip_size_mb,  # ZIP file size as placeholder
		'raw_images_path': str(final_zip_path),  # Store ZIP file path for ODM processor
		'camera_metadata': {},  # Will be populated during ODM processing
		'has_rtk_data': False,  # Will be updated during ODM processing
		'rtk_precision_cm': None,  # Will be updated during ODM processing
		'rtk_quality_indicator': None,  # Will be updated during ODM processing
		'rtk_file_count': 0,  # Will be updated during ODM processing
		'version': 1,
	}

	# Insert raw_images entry into database
	with use_client(token) as client:
		response = client.table(settings.raw_images_table).insert(raw_images_data).execute()

	# Update status to indicate upload completion only
	update_status(
		token=token,
		dataset_id=dataset.id,
		current_status=StatusEnum.idle,
		is_upload_done=True,
		has_error=False,
	)

	return dataset
