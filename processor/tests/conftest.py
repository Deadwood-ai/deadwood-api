import pytest
import shutil
from pathlib import Path


from shared.supabase import use_client, login
from shared.settings import settings

DATASET_ID = 275


@pytest.fixture(scope='session')
def auth_token():
	"""Provide authentication token for tests"""
	return login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)


@pytest.fixture
def test_file():
	"""Fixture to provide test GeoTIFF file path"""
	file_path = Path(__file__).parent / 'test_data' / 'test-data-small.tif'
	if not file_path.exists():
		pytest.skip('Test file not found')
	return file_path


@pytest.fixture()
def patch_test_file(test_file, auth_token):
	"""Fixture to provide test file for thumbnail testing"""
	# Create processing directory if it doesn't exist
	settings.processing_path.mkdir(parents=True, exist_ok=True)

	try:
		with use_client(auth_token) as client:
			response = client.table(settings.datasets_table).select('file_name').eq('id', DATASET_ID).execute()
			file_name = response.data[0]['file_name']
		if not response.data:
			pytest.skip('Dataset not found in database')

		# Copy file to processing directory
		dest_path = settings.processing_path / file_name
		shutil.copy2(test_file, dest_path)

		yield test_file

	finally:
		# Cleanup processing directory after test
		if settings.processing_path.exists():
			shutil.rmtree(settings.processing_path)
