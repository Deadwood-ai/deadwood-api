from typing import Tuple, List, Optional
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point
from shared.logger import logger
from shared.supabase import use_client
from shared.settings import settings
from shared.models import Dataset
from fastapi import HTTPException

# Define the path to the GADM database
GADM_PATH = Path('api/gadm') / 'gadm_410.gpkg'


def get_admin_tags(point: Tuple[float, float]) -> List[Optional[str]]:
	"""
	Returns administrative names for levels 0 (country), 1 (state/province),
	and 2 (municipality/district).

	Args:
	    point: Tuple of (longitude, latitude)

	Returns:
	    List of [country_name, state_name, district_name]
	"""
	try:
		# Create Point object (lon, lat)
		point_geom = Point(point[0], point[1])

		# Read only necessary columns and use spatial filter
		columns = ['NAME_0', 'NAME_2', 'NAME_4', 'geometry']
		gdf = gpd.read_file(
			GADM_PATH,
			mask=point_geom.buffer(0.1),  # Small buffer to optimize spatial query
			columns=columns,
		)

		if not gdf.empty:
			# Find the polygon containing our point
			mask = gdf.geometry.contains(point_geom)
			if mask.any():
				row = gdf[mask].iloc[0]
				return [row['NAME_0'], row['NAME_2'], row['NAME_4']]

		return [None, None, None]

	except Exception as e:
		logger.error(f'Error getting GADM admin tags: {str(e)}')
		return [None, None, None]


def update_metadata_admin_level(dataset_id: int, token: str):
	"""
	Update the admin level information in the metadata table for a given dataset.

	Args:
	    dataset_id (int): The ID of the dataset.
	    token (str): The authentication token.
	"""
	# Import here to avoid circular imports

	logger.info(f'Updating admin level information for dataset {dataset_id}', extra={'token': token})
	try:
		# Get dataset info
		with use_client(token) as client:
			response = client.table(settings.datasets_table).select('*').eq('id', dataset_id).execute()
			data = Dataset(**response.data[0])

		# Get admin tags using GADM data
		admin_levels = get_admin_tags(data.centroid)
		metadata_update = {
			'admin_level_1': admin_levels[0],  # country
			'admin_level_2': admin_levels[1],  # state/province
			'admin_level_3': admin_levels[2],  # district
		}

		# Update metadata
		with use_client(token) as client:
			client.table(settings.metadata_table).update(metadata_update).eq('dataset_id', dataset_id).execute()

		logger.info(
			f'Updated admin level information for Dataset {dataset_id}',
			extra={'token': token, 'dataset_id': dataset_id},
		)
		return metadata_update

	except Exception as e:
		logger.exception(f'Error updating admin levels for dataset {dataset_id}: {str(e)}', extra={'token': token})
		raise HTTPException(status_code=400, detail=f'Error updating admin levels for dataset {dataset_id}: {str(e)}')
