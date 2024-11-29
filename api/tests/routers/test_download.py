import pytest
import requests
from pathlib import Path
import zipfile
import json
import time
from shared.supabase import use_client
from shared.settings import settings
from fastapi.testclient import TestClient
from api.src.server import app
from fastapi import HTTPException

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

		# # Cleanup after all tests are done
		# client.table(settings.metadata_table).update(
		# 	{'admin_level_1': None, 'admin_level_2': None, 'admin_level_3': None}
		# ).eq('dataset_id', dataset_id).execute()


def test_download_dataset(auth_token, test_dataset):
	"""Test downloading a complete dataset ZIP bundle"""

	# Make request to download endpoint using TestClient
	response = client.get(
		f'/download/datasets/{test_dataset}/dataset.zip',
		headers={'Authorization': f'Bearer {auth_token}'},
		follow_redirects=True,
	)

	# Print response details for debugging
	print(f'Response status code: {response.status_code}')
	print(f'Response headers: {response.headers}')
	if response.status_code != 200:
		print(f'Response content: {response.content}')

	# Check response status
	assert response.status_code == 200, f'Request failed with status {response.status_code}'
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

	finally:
		# Cleanup
		if temp_zip.exists():
			temp_zip.unlink()
