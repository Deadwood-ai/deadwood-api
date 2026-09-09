"""
Tests for upload system - verifies upload stores files only, no technical analysis.

Key test objectives:
- GeoTIFF uploads create dataset but NO ortho entry
- ZIP uploads create dataset and raw_images entry only
- File storage in correct locations (archive/ vs raw_images/)
- Status updates (is_upload_done=True only)
- NO technical analysis during upload (ortho creation deferred to processor)
"""

from uuid import uuid4

import pytest
from pathlib import Path
import tempfile
import shutil
from fastapi.testclient import TestClient

from api.src.server import app
from api.src.utils.file_utils import UploadType
from shared.db import use_client
from shared.models import StatusEnum, LicenseEnum, PlatformEnum, DatasetAccessEnum
from shared.settings import settings

client = TestClient(app)

CHUNK_SIZE = 1024 * 1024 * 1  # 1MB chunks for testing

# Path to test data
TEST_DATA_DIR = Path('assets/test_data/raw_drone_images')
TEST_ZIP_FILE = TEST_DATA_DIR / 'test_no_rtk_3_images.zip'  # Much smaller file for faster tests


@pytest.fixture
def temp_test_zip():
	"""Create a temporary copy of test ZIP file for manipulation"""
	if not TEST_ZIP_FILE.exists():
		pytest.skip(f'Test data file not found: {TEST_ZIP_FILE}. Run `make download-assets`.')

	with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
		shutil.copy2(TEST_ZIP_FILE, tmp.name)
		yield Path(tmp.name)
		Path(tmp.name).unlink(missing_ok=True)


def test_geotiff_upload_creates_dataset_no_ortho(test_file, auth_token, test_user):
	"""Test GeoTIFF upload creates dataset but NO ortho entry"""
	# Setup
	file_size = test_file.stat().st_size
	chunks_total = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
	upload_id = f'test-geotiff-{uuid4()}'

	# Required form data
	form_data = {
		'license': LicenseEnum.cc_by.value,
		'platform': PlatformEnum.drone.value,
		'authors': ['Test Author'],
		'data_access': DatasetAccessEnum.public.value,
		'upload_type': UploadType.GEOTIFF.value,
	}

	dataset_id = None

	# Read file in chunks and upload each chunk
	with open(test_file, 'rb') as f:
		for chunk_index in range(chunks_total):
			chunk_data = f.read(CHUNK_SIZE)

			# Prepare multipart form data
			files = {'file': (f'{test_file.name}', chunk_data, 'application/octet-stream')}
			data = {
				'chunk_index': str(chunk_index),
				'chunks_total': str(chunks_total),
				'upload_id': upload_id,
				**form_data,
			}

			# Make request
			response = client.post(
				'/datasets/chunk', files=files, data=data, headers={'Authorization': f'Bearer {auth_token}'}
			)

			# Check response
			assert response.status_code == 200

			if chunk_index < chunks_total - 1:
				assert response.json() == {'message': f'Chunk {chunk_index} of {chunks_total} received'}
			else:
				# For final chunk, check dataset response
				dataset = response.json()
				dataset_id = dataset['id']

				# Verify dataset entry
				assert 'id' in dataset
				assert dataset['license'] == form_data['license']
				assert dataset['platform'] == form_data['platform']
				assert dataset['authors'] == ['Test Author']
				assert dataset['user_id'] == test_user

				# Verify file exists in correct location with correct name
				expected_filename = f'{dataset_id}_ortho.tif'
				archive_path = settings.archive_path / expected_filename
				assert archive_path.exists()
				assert archive_path.stat().st_size == file_size

				# **KEY TEST**: Verify NO ortho entry created during upload
				with use_client(auth_token) as supabase_client:
					ortho_response = (
						supabase_client.table(settings.orthos_table).select('*').eq('dataset_id', dataset_id).execute()
					)
					assert len(ortho_response.data) == 0, 'Ortho entry should NOT be created during upload'

				# Verify status indicates upload done only
				with use_client(auth_token) as supabase_client:
					status_response = (
						supabase_client.table(settings.statuses_table)
						.select('*')
						.eq('dataset_id', dataset_id)
						.execute()
					)
					assert len(status_response.data) == 1
					status = status_response.data[0]
					assert status['current_status'] == StatusEnum.idle.value
					assert status['is_upload_done'] is True
					assert status['has_error'] is False
					# Verify processing flags are still False (no processing done yet)
					assert status.get('is_cog_done', False) is False
					assert status.get('is_thumbnail_done', False) is False
					assert status.get('is_metadata_done', False) is False


def test_zip_upload_creates_dataset_and_raw_images(temp_test_zip, auth_token, test_user):
	"""Test ZIP upload creates dataset and raw_images entry only (no extraction during upload)"""
	# Setup
	file_size = temp_test_zip.stat().st_size
	chunks_total = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
	upload_id = f'test-zip-{uuid4()}'

	# Required form data
	form_data = {
		'license': LicenseEnum.cc_by.value,
		'platform': PlatformEnum.drone.value,
		'authors': ['Test Author'],
		'data_access': DatasetAccessEnum.public.value,
		'upload_type': UploadType.RAW_IMAGES_ZIP.value,
	}

	dataset_id = None

	# Read file in chunks and upload each chunk
	with open(temp_test_zip, 'rb') as f:
		for chunk_index in range(chunks_total):
			chunk_data = f.read(CHUNK_SIZE)

			# Prepare multipart form data
			files = {'file': (f'{temp_test_zip.name}', chunk_data, 'application/octet-stream')}
			data = {
				'chunk_index': str(chunk_index),
				'chunks_total': str(chunks_total),
				'upload_id': upload_id,
				**form_data,
			}

			# Make request
			response = client.post(
				'/datasets/chunk', files=files, data=data, headers={'Authorization': f'Bearer {auth_token}'}
			)

			# Check response
			assert response.status_code == 200

			if chunk_index < chunks_total - 1:
				assert response.json() == {'message': f'Chunk {chunk_index} of {chunks_total} received'}
			else:
				# For final chunk, check dataset response
				dataset = response.json()
				dataset_id = dataset['id']

				# Verify dataset entry
				assert 'id' in dataset
				assert dataset['license'] == form_data['license']
				assert dataset['platform'] == form_data['platform']
				assert dataset['authors'] == ['Test Author']
				assert dataset['user_id'] == test_user

				# Verify ZIP file stored at expected location (no extraction during upload)
				zip_filename = f'{dataset_id}.zip'
				zip_path = settings.raw_images_path / zip_filename
				assert zip_path.exists(), f'ZIP file should be stored at {zip_path}'
				assert zip_path.is_file(), 'Stored file should be a ZIP file, not a directory'

				# Verify NO extraction directory exists during upload phase
				extraction_dir = settings.raw_images_path / str(dataset_id)
				assert (
					not extraction_dir.exists()
				), 'Files should NOT be extracted during upload (deferred to ODM processing)'

				# Verify raw_images database entry created with minimal info
				with use_client(auth_token) as supabase_client:
					raw_images_response = (
						supabase_client.table(settings.raw_images_table)
						.select('*')
						.eq('dataset_id', dataset_id)
						.execute()
					)
					assert len(raw_images_response.data) == 1
					raw_images = raw_images_response.data[0]
					assert raw_images['dataset_id'] == dataset_id
					assert raw_images['raw_images_path'] == str(zip_path)  # Points to ZIP file, not directory
					assert raw_images['raw_image_count'] == 0  # Placeholder - will be updated during ODM processing
					assert raw_images['raw_image_size_mb'] > 0  # ZIP file size as placeholder

					# RTK fields should be defaults (will be updated during ODM processing)
					assert raw_images['has_rtk_data'] is False  # Default value
					assert raw_images['rtk_file_count'] == 0  # Default value
					assert raw_images['rtk_precision_cm'] is None  # Default value
					assert raw_images['rtk_quality_indicator'] is None  # Default value

				# **KEY TEST**: Verify NO ortho entry created during upload
				with use_client(auth_token) as supabase_client:
					ortho_response = (
						supabase_client.table(settings.orthos_table).select('*').eq('dataset_id', dataset_id).execute()
					)
					assert len(ortho_response.data) == 0, 'Ortho entry should NOT be created during ZIP upload'

				# Verify status indicates upload done only (processing deferred)
				with use_client(auth_token) as supabase_client:
					status_response = (
						supabase_client.table(settings.statuses_table)
						.select('*')
						.eq('dataset_id', dataset_id)
						.execute()
					)
					assert len(status_response.data) == 1
					status = status_response.data[0]
					assert status['current_status'] == StatusEnum.idle.value
					assert status['is_upload_done'] is True
					assert status['has_error'] is False


def test_upload_auto_detects_type_when_not_provided(test_file, auth_token):
	"""Test that upload_type is auto-detected when not explicitly provided (backward compatibility)"""
	form_data = {
		'chunk_index': '0',
		'chunks_total': '1',
		'upload_id': f'test-auto-detect-{uuid4()}',
		'license': LicenseEnum.cc_by.value,
		'platform': PlatformEnum.drone.value,
		'authors': ['Test Author'],
		'data_access': DatasetAccessEnum.public.value,
		# Note: upload_type not provided - should be auto-detected
	}

	with open(test_file, 'rb') as f:
		chunk_data = f.read(1024)  # Small chunk for test
		files = {'file': (f'{test_file.name}', chunk_data, 'application/octet-stream')}

		response = client.post(
			'/datasets/chunk', files=files, data=form_data, headers={'Authorization': f'Bearer {auth_token}'}
		)

		# Should succeed - type auto-detected from .tif extension
		assert response.status_code == 200


def test_upload_without_auth():
	"""Test upload attempt without authentication"""
	response = client.post(
		'/datasets/chunk',
		files={'file': ('test.tif', b'test data', 'application/octet-stream')},
		data={
			'chunk_index': '0',
			'chunks_total': '1',
			'upload_id': f'test-{uuid4()}',
			'license': LicenseEnum.cc_by.value,
			'platform': PlatformEnum.drone.value,
			'authors': ['Test Author'],
		},
	)
	assert response.status_code == 401
