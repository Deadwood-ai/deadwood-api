from datetime import datetime, timezone
from typing import Optional
from .models import StatusEnum
from .db import use_client
from .settings import settings
from .logger import logger
from .retry import retry_on_transient_error
from shared.logging import LogContext, LogCategory


def update_status(
	token: str,
	dataset_id: int,
	current_status: Optional[StatusEnum] = None,
	is_upload_done: Optional[bool] = None,
	is_ortho_done: Optional[bool] = None,
	is_cog_done: Optional[bool] = None,
	is_thumbnail_done: Optional[bool] = None,
	is_deadwood_done: Optional[bool] = None,
	is_forest_cover_done: Optional[bool] = None,
	is_combined_model_done: Optional[bool] = None,
	is_aoi_done: Optional[bool] = None,
	is_aoi_required: Optional[bool] = None,
	is_embeddings_done: Optional[bool] = None,
	is_metadata_done: Optional[bool] = None,
	is_odm_done: Optional[bool] = None,
	has_error: Optional[bool] = None,
	error_message: Optional[str] = None,
	error_stage: Optional[str] = None,
) -> None:
	"""Update the status fields of a dataset in the statuses table.
	Only provided fields will be updated.

	Args:
	    token (str): Supabase client session token
	    dataset_id (int): Unique id of the dataset
	    current_status (StatusEnum, optional): Current processing status
	    is_upload_done (bool, optional): Upload completion status
	    is_ortho_done (bool, optional): Ortho processing completion status
	    is_cog_done (bool, optional): COG processing completion status
	    is_thumbnail_done (bool, optional): Thumbnail generation completion status
	    is_deadwood_done (bool, optional): Deadwood segmentation completion status
	    is_forest_cover_done (bool, optional): Forest cover completion status
	    is_combined_model_done (bool, optional): Combined model completion status
	    is_aoi_done (bool, optional): Automatic AOI segmentation completion status
	    is_aoi_required (bool, optional): Whether automatic AOI completion is required for this dataset
	    is_metadata_done (bool, optional): Metadata processing completion status
	    is_odm_done (bool, optional): ODM processing completion status
	    has_error (bool, optional): Error status flag
	    error_message (str, optional): Error message if any
	"""
	try:
		update_data = {}
		if current_status is not None:
			update_data['current_status'] = current_status
		if is_upload_done is not None:
			update_data['is_upload_done'] = is_upload_done
		if is_ortho_done is not None:
			update_data['is_ortho_done'] = is_ortho_done
		if is_cog_done is not None:
			update_data['is_cog_done'] = is_cog_done
		if is_thumbnail_done is not None:
			update_data['is_thumbnail_done'] = is_thumbnail_done
		if is_deadwood_done is not None:
			update_data['is_deadwood_done'] = is_deadwood_done
		if is_forest_cover_done is not None:
			update_data['is_forest_cover_done'] = is_forest_cover_done
		if is_combined_model_done is not None:
			update_data['is_combined_model_done'] = is_combined_model_done
		if is_aoi_done is not None:
			update_data['is_aoi_done'] = is_aoi_done
		if is_aoi_required is not None:
			update_data['is_aoi_required'] = is_aoi_required
		if is_embeddings_done is not None:
			update_data['is_embeddings_done'] = is_embeddings_done
		if is_metadata_done is not None:
			update_data['is_metadata_done'] = is_metadata_done
		if is_odm_done is not None:
			update_data['is_odm_done'] = is_odm_done
		if has_error is not None:
			update_data['has_error'] = has_error
		if has_error is False:
			update_data['error_stage'] = None
		elif has_error is True or error_stage is not None:
			update_data['error_stage'] = error_stage
		if error_message is not None:
			update_data['error_message'] = error_message

		if update_data:
			update_data['updated_at'] = datetime.now(timezone.utc).isoformat()

			@retry_on_transient_error
			def _write_status() -> None:
				with use_client(token) as client:
					# First check if status exists
					result = client.table(settings.statuses_table).select('id').eq('dataset_id', dataset_id).execute()

					if not result.data:
						# Create new status row if it doesn't exist
						client.table(settings.statuses_table).insert({'dataset_id': dataset_id, **update_data}).execute()
					else:
						# Update existing status
						client.table(settings.statuses_table).update(update_data).eq('dataset_id', dataset_id).execute()

			_write_status()

	except Exception as e:
		logger.error(
			f'Error updating status: {e}', LogContext(category=LogCategory.STATUS, dataset_id=dataset_id, token=token)
		)
		raise
