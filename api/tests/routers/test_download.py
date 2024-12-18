import pytest
from pathlib import Path
import zipfile
from shapely.geometry import Polygon
import shutil

from shared.supabase import use_client
from shared.settings import settings
from fastapi.testclient import TestClient
from api.src.server import app

client = TestClient(app)


@pytest.fixture(scope='function')
def test_dataset_for_download(auth_token, mock_data_directory, test_file):
	"""Create a temporary test dataset for download testing"""
	with use_client(auth_token) as client:
		# Copy test file to mock archive directory
		file_name = 'test-download.tif'
		archive_path = mock_data_directory / file_name
		shutil.copy2(test_file, archive_path)

		# Create test dataset
		dataset_data = {
			'file_name': file_name,
			'file_alias': file_name,
			'file_size': archive_path.stat().st_size,
			'copy_time': 123,
			'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
			'status': 'uploaded',
		}
		response = client.table(settings.datasets_table).insert(dataset_data).execute()
		dataset_id = response.data[0]['id']

		# Create metadata
		metadata_data = {
			'dataset_id': dataset_id,
			'name': 'Test Dataset',
			'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
			'authors': 'Test Author',
			'platform': 'drone',
			'data_access': 'public',
			'license': 'CC BY',
			'aquisition_year': 2024,
			'aquisition_month': 1,
			'aquisition_day': 1,
		}
		client.table(settings.metadata_table).insert(metadata_data).execute()

		yield dataset_id

		# Cleanup
		client.table(settings.metadata_table).delete().eq('dataset_id', dataset_id).execute()
		client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


def test_download_dataset(auth_token, test_dataset_for_download):
	"""Test downloading a complete dataset ZIP bundle"""
	# Make request to download endpoint using TestClient
	response = client.get(
		f'/download/datasets/{test_dataset_for_download}/dataset.zip',
		headers={'Authorization': f'Bearer {auth_token}'},
		follow_redirects=True,
	)

	# Check response status
	assert response.status_code == 200
	assert response.headers['content-type'] == 'application/zip'

	# Save response content to temporary file and verify ZIP contents
	temp_zip = Path('test_download.zip')
	try:
		temp_zip.write_bytes(response.content)

		with zipfile.ZipFile(temp_zip) as zf:
			# List all files in the ZIP
			files = zf.namelist()

			# Verify expected files
			assert any(f.startswith('ortho_') and f.endswith('.tif') for f in files)
			assert 'METADATA.csv' in files
			assert 'METADATA.parquet' in files
			assert 'LICENSE.txt' in files

	finally:
		# Cleanup
		if temp_zip.exists():
			temp_zip.unlink()


@pytest.fixture(scope='function')
def test_dataset_with_label(auth_token, mock_data_directory, test_file):
	"""Create a temporary test dataset with metadata and label for testing downloads"""
	# Create test dataset
	with use_client(auth_token) as client:
		# Copy test file to mock archive directory with the same name we're using
		file_name = 'test-download-label.tif'
		archive_path = mock_data_directory / file_name
		shutil.copy2(test_file, archive_path)

		dataset_data = {
			'file_name': file_name,  # Use the same filename
			'file_alias': file_name,
			'file_size': archive_path.stat().st_size,
			'copy_time': 123,
			'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
			'status': 'uploaded',
		}
		response = client.table(settings.datasets_table).insert(dataset_data).execute()
		dataset_id = response.data[0]['id']

		# Create metadata
		metadata_data = {
			'dataset_id': dataset_id,
			'name': 'Test Dataset',
			'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
			'authors': 'Test Author',
			'admin_level_1': 'Test Admin Level 1',
			'admin_level_2': 'Test Admin Level 2',
			'admin_level_3': 'Test Admin Level 3',
			'platform': 'drone',
			'data_access': 'public',
			'license': 'CC BY',
			'aquisition_year': 2024,
			'aquisition_month': 1,
			'aquisition_day': 1,
		}
		client.table(settings.metadata_table).insert(metadata_data).execute()

		# Create a simple polygon for the label
		polygon = Polygon([[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]])
		multipolygon_geojson = {
			'type': 'MultiPolygon',
			'coordinates': [[polygon.exterior.coords[:]]],
		}
		label_multipolygon = {
			'type': 'MultiPolygon',
			'coordinates': [[[[0, 0], [0, 0.5], [0.5, 0.5], [0.5, 0], [0, 0]]]],
		}

		# Create label
		label_data = {
			'dataset_id': dataset_id,
			'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
			'aoi': multipolygon_geojson,
			'label': label_multipolygon,
			'label_source': 'visual_interpretation',
			'label_quality': 1,
			'label_type': 'segmentation',
		}
		client.table(settings.labels_table).insert(label_data).execute()

		yield dataset_id

		# Cleanup
		client.table(settings.labels_table).delete().eq('dataset_id', dataset_id).execute()
		client.table(settings.metadata_table).delete().eq('dataset_id', dataset_id).execute()
		client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


def test_download_dataset_with_labels(auth_token, test_dataset_with_label):
	"""Test downloading a dataset that includes labels"""

	# Make request to download endpoint using TestClient
	response = client.get(
		f'/download/datasets/{test_dataset_with_label}/dataset.zip',
		headers={'Authorization': f'Bearer {auth_token}'},
		follow_redirects=True,
	)

	# Check response status
	assert response.status_code == 200
	assert response.headers['content-type'] == 'application/zip'

	# Save response content to temporary file and verify ZIP contents
	temp_zip = Path('test_download_with_labels.zip')
	try:
		temp_zip.write_bytes(response.content)

		with zipfile.ZipFile(temp_zip) as zf:
			# List all files in the ZIP
			files = zf.namelist()
			# Check for files with new naming pattern
			assert any(f.startswith('ortho_') and f.endswith('.tif') for f in files)
			assert any(f.startswith('labels_') and f.endswith('.gpkg') for f in files)
			assert 'METADATA.csv' in files
			assert 'METADATA.parquet' in files
			assert 'LICENSE.txt' in files

	finally:
		# Cleanup
		if temp_zip.exists():
			temp_zip.unlink()
