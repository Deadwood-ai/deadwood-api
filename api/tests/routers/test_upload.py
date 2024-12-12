import pytest
from pathlib import Path
import tempfile
import shutil
from fastapi.testclient import TestClient
from api.src.server import app
from shared.supabase import login, use_client
from shared.settings import settings

client = TestClient(app)


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


@pytest.fixture
def test_file():
	"""Fixture to provide test GeoTIFF file path"""
	file_path = Path(__file__).parent.parent / 'test_data' / 'test-data-small.tif'
	if not file_path.exists():
		pytest.skip('Test file not found')
	return file_path


@pytest.fixture
def auth_token():
	"""Fixture to provide authentication token"""
	return login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)


@pytest.fixture
def temp_upload_dir():
	"""Fixture to provide temporary directory for uploads"""
	temp_dir = tempfile.mkdtemp()
	# Update settings to use temp directory
	original_base_dir = settings.BASE_DIR
	settings.BASE_DIR = temp_dir

	yield temp_dir

	# Cleanup
	settings.BASE_DIR = original_base_dir
	shutil.rmtree(temp_dir)


def test_upload_geotiff_chunk(test_file, auth_token, temp_upload_dir):
	"""Test chunked upload of a GeoTIFF file"""
	# Create necessary subdirectories
	archive_dir = Path(temp_upload_dir) / settings.ARCHIVE_DIR
	archive_dir.mkdir(parents=True, exist_ok=True)

	# Setup
	chunk_size = 1024 * 1024  # 1MB chunks for testing
	file_size = test_file.stat().st_size
	chunks_total = (file_size + chunk_size - 1) // chunk_size
	upload_id = 'test-upload-id'

	# Read file in chunks and upload each chunk
	with open(test_file, 'rb') as f:
		for chunk_index in range(chunks_total):
			chunk_data = f.read(chunk_size)

			# Prepare multipart form data
			files = {'file': ('test-data.tif', chunk_data, 'application/octet-stream')}
			data = {
				'chunk_index': str(chunk_index),
				'chunks_total': str(chunks_total),
				'filename': test_file.name,
				'copy_time': '0',
				'upload_id': upload_id,
			}

			# Make request
			response = client.post(
				'/datasets/chunk', files=files, data=data, headers={'Authorization': f'Bearer {auth_token}'}
			)

			# Check response
			assert response.status_code == 200

			# For intermediate chunks, check message
			if chunk_index < chunks_total - 1:
				assert response.json() == {'message': f'Chunk {chunk_index} of {chunks_total} received'}
			else:
				# For final chunk, check dataset response
				dataset = response.json()
				dataset_id = dataset['id']
				assert 'id' in dataset
				assert dataset['file_alias'] == test_file.name
				assert dataset['status'] == 'uploaded'
				assert dataset['file_size'] > 0
				assert dataset['bbox'] is not None
				assert dataset['sha256'] is not None
				assert isinstance(dataset['file_name'], str)
				assert '/' not in dataset['file_name']

				# Cleanup: Remove the dataset from the database if it was created
				if dataset_id:
					with use_client(auth_token) as supabase_client:
						# Delete from metadata table first (due to foreign key constraint)
						supabase_client.table(settings.metadata_table).delete().eq('dataset_id', dataset_id).execute()
						# Delete from datasets table
						supabase_client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


def test_upload_without_auth():
	"""Test upload attempt without authentication"""
	response = client.post(
		'/datasets/chunk',
		files={'file': ('test.tif', b'test data', 'application/octet-stream')},
		data={
			'chunk_index': '0',
			'chunks_total': '1',
			'filename': 'test.tif',
			'copy_time': '0',
			'upload_id': 'test',
		},
	)
	assert response.status_code == 401
