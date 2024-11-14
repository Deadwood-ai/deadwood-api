from fastapi import HTTPException

from shared.supabase import use_client
from shared.settings import settings
from shared.models import Dataset
from .osm import get_admin_tags
from shared.logger import logger


def update_metadata_admin_level(dataset_id: int, token: str):
	"""
	Update the admin level information in the metadata table for a given dataset.

	Args:
		dataset_id (int): The ID of the dataset.
		token (str): The authentication token.
	"""
	try:
		# Calculate the centroid of the bounding box
		with use_client(token) as client:
			response = client.table(settings.datasets_table).select('*').eq('id', dataset_id).execute()
			data = Dataset(**response.data[0])
	except Exception as e:
		logger.exception(f'Error getting dataset {dataset_id}: {str(e)}', extra={'token': token})
		raise HTTPException(status_code=400, detail=f'Error getting dataset {dataset_id}: {str(e)}')

	# Get the admin tags for the centroid
	try:
		(lvl1, lvl2, lvl3) = get_admin_tags(data.centroid)
	except Exception as e:
		logger.exception(f'Error getting admin tags for dataset {dataset_id}: {str(e)}', extra={'token': token})
		raise HTTPException(status_code=400, detail=f'Error getting admin tags for dataset {dataset_id}: {str(e)}')

	try:
		with use_client(token) as client:
			# Update the metadata in the database
			metadata_update = {
				'admin_level_1': lvl1,
				'admin_level_2': lvl2,
				'admin_level_3': lvl3,
			}
			client.table(settings.metadata_table).update(metadata_update).eq('dataset_id', dataset_id).execute()

	except Exception as e:
		logger.error(
			f'An error occurred while updating admin level information for dataset_id {dataset_id}: {str(e)}',
			extra={'token': token, 'dataset_id': dataset_id},
		)
	logger.info(
		f'Updated admin level information for Dataset {dataset_id}',
		extra={'token': token, 'dataset_id': dataset_id},
	)
	return True
