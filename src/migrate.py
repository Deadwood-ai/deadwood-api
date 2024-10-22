"""
Migration script to import the old database into the new one.

"""

import os
from functools import cache
from pathlib import Path
import time

import httpx

from .supabase import login, use_client
from .logger import logger
from .models import MetadataPayloadData, LabelPayloadData, Dataset


@cache
def get_old_env(**kwargs) -> dict:
	"""
	Connect to the old environment that needs to be migrated.
	"""
	# tables
	metadata_table = kwargs.get('metadata_table', os.environ.get('OLD_METADATA_TABLE', 'metadata_dev_egu_view'))
	labels_table = kwargs.get('labels_table', os.environ.get('OLD_LABELS_TABLE', 'labels_dev_egu'))
	migration_table = kwargs.get('migration_table', os.environ.get('OLD_MIGRATION_TABLE', 'migrate_v1'))

	# file locations
	archive_path = kwargs.get(
		'archive_path', os.environ.get('OLD_ARCHIVE_PATH', '/apps/storage-server/dev-data/archive')
	)

	# new user
	supabase_user = kwargs.get('processor_user', os.environ.get('PROCESSOR_USERNAME'))
	supabase_password = kwargs.get('processor_password', os.environ.get('PROCESSOR_PASSWORD'))
	token = kwargs.get('token')

	# set the API url
	api_url = kwargs.get('api_url', os.environ.get('API_URL', 'http://api:8762'))

	# get a token
	if token is None:
		if supabase_user is None or supabase_password is None:
			raise ValueError('No token or user/password provided.')
		else:
			token = login(supabase_user, supabase_password)

	return dict(
		metadata_table=metadata_table,
		labels_table=labels_table,
		migration_table=migration_table,
		archive_path=archive_path,
		api_url=api_url,
		token=token,
	)


def get_next_dataset() -> str:
	"""
	Check the database to find the next file in the old database, that
	does not exist in the new database.
	"""
	# get the token
	token = get_old_env()['token']
	table = get_old_env()['migration_table']

	# query the migration table
	with use_client(token) as db:
		response = db.table(table).select('*').limit(1).execute()

	return response.data[0]


def merge_multipolygon_geometries(feature_collection: dict) -> dict:
	coords = []
	for feature in feature_collection['features']:
		coords.extend(feature['geometry']['coordinates'])

	return {'type': 'MultiPolygon', 'coordinates': coords}


def migrate_file(old_metadata: dict) -> None:
	""" """
	# get the config
	conf = get_old_env()

	# get the path to the old file
	origin_path = Path(conf['archive_path']) / old_metadata['file_id']

	if not origin_path.exists():
		msg = f'Cannot migrage file {origin_path}. File does not exist.'
		logger.error(msg, extra={'token': conf['token']})
		return

	# start the timer
	t1 = time.time()

	# for all the requests build a header
	header = {'Authorization': f"Bearer {conf['token']}"}

	# upload the new file
	try:
		# set the file object
		files = {'file': (old_metadata['file_name'], origin_path.open('rb'))}
		response = httpx.post(f"{conf['api_url']}/datasets", files=files, headers=header, timeout=60 * 2)
		dataset = Dataset(**response.json())

	except Exception as e:
		logger.exception(
			f'An error occurred while trying to upload the dataset: {str(e)}', extra={'token': conf['token']}
		)
		return

	# get the metadata
	metadata = MetadataPayloadData(
		name=old_metadata['file_name'],
		license=old_metadata['license'],
		platform=old_metadata['platform'],
		authors=old_metadata['authors_image'],
		spectral_properties=old_metadata['image_spectral_properties'],
		citation_doi=old_metadata['citation_doi'],
		gadm_name_1=old_metadata['gadm_NAME_0'],
		gadm_name_2=old_metadata['gadm_NAME_1'],
		gadm_name_3=old_metadata['gadm_NAME_2'],
		aquisition_date=old_metadata['aquisition_date'],
	)

	# send the metadata
	try:
		response = httpx.put(
			f"{conf['api_url']}/datasets/{dataset.id}/metadata", json=metadata.model_dump(), headers=header, timeout=10
		)
		print(f'Saved metadata: {response.json()}')
	except Exception as e:
		logger.exception(
			f'An error occurred while trying to upload the metadata: {str(e)}', extra={'token': conf['token']}
		)
		return

	# build the new label
	if 'features' not in old_metadata['aoi'] or len(old_metadata['aoi']['features']) == 0:
		logger.warning(
			f'No AOI found for dataset {dataset.id}. Skipping label creation.', extra={'token': conf['token']}
		)
	else:
		label = LabelPayloadData(
			aoi=old_metadata['aoi']['features'][0]['geometry'],
			label=merge_multipolygon_geometries(old_metadata['standing_deadwood']),
			label_source=old_metadata['label_source'],
			label_quality=int(float(old_metadata['label_quality'])),
			label_type=old_metadata['label_type'],
		)

		# send the label
		try:
			response = httpx.post(
				f"{conf['api_url']}/datasets/{dataset.id}/labels", json=label.model_dump(), headers=header, timeout=10
			)
		except Exception as e:
			logger.exception(
				f'An error occurred while trying to upload the label: {str(e)}', extra={'token': conf['token']}
			)

	# build the new cog
	try:
		response = httpx.put(
			f"{conf['api_url']}/datasets/{dataset.id}/build-cog", json=dict(), headers=header, timeout=None
		)
		print(f'COG build response: {response.json()}')
	except Exception as e:
		logger.exception(f'An error occurred while trying to build the COG: {str(e)}', extra={'token': conf['token']})
		return

	t2 = time.time()

	logger.info(
		f'Migration for new Dataset <ID={dataset.id}> successfull after {t2 - t1} seconds.',
		extra={'token': conf['token']},
	)
