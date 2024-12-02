import pytest
from fastapi.testclient import TestClient

from api.src.server import app
from shared.supabase import use_client
from shared.settings import settings
from shared.models import MetadataPayloadData, PlatformEnum, DatasetAccessEnum

client = TestClient(app)


@pytest.fixture(scope='function')
def test_dataset(auth_token):
	"""Create a temporary test dataset for metadata testing"""
	# Create test dataset
	with use_client(auth_token) as client:
		dataset_data = {
			'file_name': 'test-metadata.tif',
			'file_alias': 'test-metadata.tif',
			'file_size': 1000,
			'copy_time': 123,
			'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
			'status': 'uploaded',
		}
		response = client.table(settings.datasets_table).insert(dataset_data).execute()
		dataset_id = response.data[0]['id']

		yield dataset_id

		# Cleanup: Remove test dataset and any associated metadata
		client.table(settings.metadata_table).delete().eq('dataset_id', dataset_id).execute()
		client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


def test_upsert_metadata(test_dataset, auth_token):
	"""Test upserting metadata for a dataset"""
	# Prepare test metadata
	metadata_payload = MetadataPayloadData(
		name='Test Dataset',
		platform=PlatformEnum.drone,
		data_access=DatasetAccessEnum.public,
		authors='Test Author',
		aquisition_year=2024,
		aquisition_month=1,
		aquisition_day=1,
	)

	# Make request to upsert metadata
	response = client.put(
		f'/datasets/{test_dataset}/metadata',
		json=metadata_payload.model_dump(),
		headers={'Authorization': f'Bearer {auth_token}'},
	)

	# Check response
	assert response.status_code == 200
	data = response.json()

	# Verify the metadata was correctly saved
	assert data['name'] == metadata_payload.name
	assert data['platform'] == metadata_payload.platform
	assert data['data_access'] == metadata_payload.data_access
	assert data['authors'] == metadata_payload.authors
	assert data['aquisition_year'] == metadata_payload.aquisition_year
	assert data['dataset_id'] == test_dataset


def test_upsert_metadata_unauthorized():
	"""Test metadata upsert without authentication"""
	response = client.put(
		'/datasets/1/metadata',
		json={},
	)
	assert response.status_code == 401


def test_upsert_metadata_invalid_dataset(auth_token):
	"""Test metadata upsert for non-existent dataset"""
	metadata_payload = MetadataPayloadData(
		name='Test Dataset',
		platform=PlatformEnum.drone,
		data_access=DatasetAccessEnum.public,
		authors='Test Author',
		aquisition_year=2024,
	)

	response = client.put(
		'/datasets/99999/metadata',  # Non-existent dataset ID
		json=metadata_payload.model_dump(),
		headers={'Authorization': f'Bearer {auth_token}'},
	)
	assert response.status_code == 400
