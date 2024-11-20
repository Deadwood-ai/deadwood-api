import pytest
from pathlib import Path
from shared.supabase import use_client, login
from shared.settings import settings


@pytest.fixture(scope='session')
def gadm_path():
	"""Ensure GADM data is available for tests"""
	path = Path('/gadm/gadm_410.gpkg')
	if not path.exists():
		pytest.skip('GADM data not available')
	return path


@pytest.fixture(scope='session')
def auth_token():
	"""Provide authentication token for tests"""
	return login(settings.processor_username, settings.processor_password)


@pytest.fixture(scope='session')
def test_dataset(auth_token):
	"""Get the test dataset ID from the database.
	Expects a dataset with file_alias = 'test-data.tif' to exist.
	"""
	with use_client(auth_token) as client:
		response = client.table(settings.datasets_table).select('id').eq('file_alias', 'test-data.tif').execute()

		if not response.data:
			pytest.skip('Test dataset not found in database')

		dataset_id = response.data[0]['id']

		yield dataset_id  # Return the dataset ID for the test

		# Cleanup after all tests are done
		client.table(settings.metadata_table).update(
			{'admin_level_1': None, 'admin_level_2': None, 'admin_level_3': None}
		).eq('dataset_id', dataset_id).execute()
