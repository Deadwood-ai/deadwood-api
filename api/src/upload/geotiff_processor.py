"""Simplified GeoTIFF upload processing - focuses on file storage only"""

from pathlib import Path
from shared.models import Dataset
from shared.status import update_status
from shared.models import StatusEnum
from shared.settings import settings


def process_geotiff_upload(dataset: Dataset, upload_target_path: Path, token: str) -> Dataset:
	"""Process GeoTIFF upload with simplified logic - only file storage, no technical analysis

	Args:
	    dataset: The dataset object created during upload
	    upload_target_path: Path to the uploaded temporary file
	    token: Authentication token for database operations

	Returns:
	    Dataset: The updated dataset object

	Note:
	    This function only handles file storage. All technical analysis (hash calculation,
	    cog_info analysis, ortho entry creation) is deferred to the geotiff processing task.
	"""
	# Move file to standard archive location
	file_name = f'{dataset.id}_ortho.tif'
	target_path = settings.archive_path / file_name
	upload_target_path.rename(target_path)

	# Update status to indicate upload completion only
	update_status(
		token=token,
		dataset_id=dataset.id,
		current_status=StatusEnum.idle,
		is_upload_done=True,
		has_error=False,
	)

	return dataset
