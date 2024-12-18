import pytest
import tempfile
import shutil
from pathlib import Path
from shared.supabase import use_client, login
from shared.settings import settings
from unittest.mock import patch


DATASET_ID = 275


@pytest.fixture
def test_file():
	"""Fixture to provide test GeoTIFF file path"""
	file_path = Path(__file__).parent / 'test_data' / 'test-data-small.tif'
	if not file_path.exists():
		pytest.skip('Test file not found')
	return file_path


@pytest.fixture(scope='session')
def auth_token():
	"""Provide authentication token for tests"""
	return login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)


@pytest.fixture(autouse=True)
def mock_data_directory(test_file):
	"""Replace /data with a temporary directory during tests"""
	with tempfile.TemporaryDirectory() as temp_dir:
		temp_path = Path(temp_dir)

		# Create a mock property that returns our temp path
		def get_base_path(self):
			return temp_path

		def get_archive_path(self):
			return temp_path

		# Patch both properties
		with patch('shared.settings.Settings.base_path', new_callable=property, fget=get_base_path):
			with patch('shared.settings.Settings.archive_path', new_callable=property, fget=get_archive_path):
				yield temp_path  # Return the temp_path instead of temp_dir
