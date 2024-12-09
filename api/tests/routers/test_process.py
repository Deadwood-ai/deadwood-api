import pytest
from fastapi.testclient import TestClient

from api.src.server import app
from shared.supabase import use_client
from shared.settings import settings
from shared.models import TaskTypeEnum

client = TestClient(app)


@pytest.fixture(scope='function')
def test_dataset(auth_token):
	"""Create a temporary test dataset for process testing"""
	dataset_id = None

	try:
		# Create test dataset
		with use_client(auth_token) as supabaseClient:
			dataset_data = {
				'file_name': 'test-process.tif',
				'file_alias': 'test-process.tif',
				'file_size': 1000,
				'copy_time': 123,
				'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
				'status': 'uploaded',
			}
			response = supabaseClient.table(settings.datasets_table).insert(dataset_data).execute()
			dataset_id = response.data[0]['id']

			yield dataset_id

	finally:
		# Ensure cleanup happens even if tests fail
		if dataset_id:
			with use_client(auth_token) as supabaseClient:
				# Delete from queue table first (this will cascade to queue_positions view)
				supabaseClient.table(settings.queue_table).delete().eq('dataset_id', dataset_id).execute()
				# Delete the dataset
				supabaseClient.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


def test_create_processing_task(test_dataset, auth_token):
	"""Test creating a new processing task for a dataset"""
	# Make request to create processing task
	response = client.put(
		f'/datasets/{test_dataset}/process',
		params={'task_type': 'cog'},
		headers={'Authorization': f'Bearer {auth_token}'},
	)

	# Check response
	assert response.status_code == 200
	data = response.json()

	# Verify the task was correctly saved
	assert data['dataset_id'] == test_dataset
	assert data['task_type'] == TaskTypeEnum.cog
	assert not data['is_processing']
	assert 'current_position' in data
	assert 'estimated_time' in data

	# Verify task exists in database
	with use_client(auth_token) as supabaseClient:
		response = supabaseClient.table(settings.queue_table).select('*').eq('dataset_id', test_dataset).execute()
		assert len(response.data) == 1
		assert response.data[0]['dataset_id'] == test_dataset
		assert response.data[0]['task_type'] == TaskTypeEnum.cog


def test_create_processing_task_unauthorized(test_dataset):
	"""Test process creation without authentication"""
	response = client.put(
		f'/datasets/{test_dataset}/process',
		params={'task_type': 'cog'},
		headers={},
	)
	assert response.status_code == 401


def test_create_processing_task_invalid_dataset(auth_token):
	"""Test process creation for non-existent dataset"""
	response = client.put(
		'/datasets/99999/process',  # Non-existent dataset ID
		params={'task_type': 'cog'},
		headers={'Authorization': f'Bearer {auth_token}'},
	)
	assert response.status_code == 404
