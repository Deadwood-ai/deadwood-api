import pytest
import requests
from pathlib import Path
import zipfile
import json
import time


@pytest.fixture
def api_url():
	return 'http://localhost:8762'  # Match the port from docker-compose.api.yaml


def test_download_dataset(api_url, auth_token, test_dataset):
	"""Test downloading a complete dataset ZIP bundle against live server"""

	# Make request to download endpoint
	response = requests.get(
		f'{api_url}/download/datasets/{test_dataset}/dataset.zip', headers={'Authorization': f'Bearer {auth_token}'}
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
			assert any(f.endswith('.tif') for f in files)
			assert any(f.endswith('.json') and not f.endswith('schema.json') for f in files)
			assert any(f.endswith('schema.json') for f in files)
			assert 'LICENSE.txt' in files
			assert 'CITATION.cff' in files

			# Verify metadata JSON content
			metadata_file = next(f for f in files if f.endswith('.json') and not f.endswith('schema.json'))
			metadata_content = json.loads(zf.read(metadata_file))
			assert metadata_content['dataset_id'] == test_dataset

	finally:
		# Cleanup
		if temp_zip.exists():
			temp_zip.unlink()


# import pytest
# from fastapi.testclient import TestClient
# from pathlib import Path
# import zipfile
# import json

# from api.src.server import app
# from shared.models import Dataset, Metadata
# from shared.settings import settings

# client = TestClient(app)


# def test_download_dataset_zip(auth_token, test_dataset):
# 	"""Test downloading a complete dataset ZIP bundle"""

# 	# Make request to download endpoint
# 	response = client.get(f'/datasets/{test_dataset}/dataset.zip', headers={'Authorization': f'Bearer {auth_token}'})

# 	# Check response status
# 	assert response.status_code == 200
# 	assert response.headers['content-type'] == 'application/zip'

# 	# Save response content to temporary file and verify ZIP contents
# 	temp_zip = Path('test_download.zip')
# 	try:
# 		temp_zip.write_bytes(response.content)

# 		with zipfile.ZipFile(temp_zip) as zf:
# 			# List all files in the ZIP
# 			files = zf.namelist()

# 			# There should be at least these files:
# 			# - The GeoTIFF
# 			# - metadata.json
# 			# - metadata schema
# 			# - LICENSE.txt
# 			# - CITATION.cff
# 			assert any(f.endswith('.tif') for f in files)
# 			assert any(f.endswith('.json') and not f.endswith('schema.json') for f in files)
# 			assert any(f.endswith('schema.json') for f in files)
# 			assert 'LICENSE.txt' in files
# 			assert 'CITATION.cff' in files

# 			# Verify metadata JSON content
# 			metadata_file = next(f for f in files if f.endswith('.json') and not f.endswith('schema.json'))
# 			metadata_content = json.loads(zf.read(metadata_file))
# 			assert metadata_content['dataset_id'] == test_dataset

# 	finally:
# 		# Cleanup
# 		if temp_zip.exists():
# 			temp_zip.unlink()


# def test_download_dataset_not_found():
# 	"""Test downloading a non-existent dataset"""
# 	response = client.get('/datasets/99999999/dataset.zip')
# 	assert response.status_code == 404
# 	assert 'not found' in response.json()['detail'].lower()


# def test_download_dataset_no_metadata():
# 	"""Test downloading a dataset without metadata"""
# 	# Create dataset without metadata
# 	dataset = Dataset(id=999, file_name='test.tif')

# 	response = client.get(f'/datasets/{dataset.id}/dataset.zip')
# 	assert response.status_code == 404
# 	assert 'no associated metadata' in response.json()['detail'].lower()
