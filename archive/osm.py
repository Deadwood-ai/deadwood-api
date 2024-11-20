from typing import Tuple
import overpy
from fastapi import HTTPException

from shared.supabase import use_client
from shared.settings import settings
from shared.models import Dataset
from shared.logger import logger


QUERY_TEMPLATE = """
[out:json];is_in({lat},{lon});rel(pivot)->.w;
.w out tags;
"""


def get_admin_level_from_point(point: Tuple[float, float]):
	# initialize the Overpass API
	api = overpy.Overpass()

	# TODO: here we can also check the point and transform etc.
	lon = point[0]
	lat = point[1]

	# build the query
	query = QUERY_TEMPLATE.format(lon=lon, lat=lat)
	result = api.query(query)

	# filter the stuff
	out = []
	for relation in result.relations:
		# check if this is an administrative boundary
		if not relation.tags.get('boundary') == 'administrative':
			continue

		# get the stuff
		admin_level = relation.tags.get('admin_level')
		if admin_level == '2':
			name = relation.tags.get('name:en', relation.tags.get('name'))
		else:
			name = relation.tags.get('name')

		# continue is info is missing
		if name is None or admin_level is None:
			continue

		# append the info
		out.append({'name': name, 'admin_level': admin_level})

	return out


def get_admin_tags(point: Tuple[float, float]):
	"""
	Returns the level 2, 4 and either 6,7 or 8, depending on
	the availability.
	"""
	tags = get_admin_level_from_point(point)

	levels = [tag['admin_level'] for tag in tags]
	names = [tag['name'] for tag in tags]

	out = [
		names[levels.index('2')],
		names[levels.index('4')] if '4' in levels else None,
	]

	if any(level in levels for level in ['6', '7', '8']):
		out.append(
			names[levels.index('8')]
			if '8' in levels
			else names[levels.index('7')]
			if '7' in levels
			else names[levels.index('6')]
		)
	else:
		out.append(None)

	return out


def update_metadata_admin_level(dataset_id: int, token: str):
	"""
	Update the admin level information in the metadata table for a given dataset.

	Args:
		dataset_id (int): The ID of the dataset.
		token (str): The authentication token.
	"""
	logger.info(f'Updating admin level information for dataset {dataset_id}', extra={'token': token})
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
		logger.info(f'Admin tags for dataset {dataset_id}: {lvl1}, {lvl2}, {lvl3}', extra={'token': token})
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
	return metadata_update


# if __name__ == '__main__':
# 	point = (8.89393, 51.405)
# 	print(get_admin_tags(point))
