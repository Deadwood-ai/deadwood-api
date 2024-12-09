import pytest
from fastapi.testclient import TestClient
from shapely.geometry import Polygon
import io
from pathlib import Path
from datetime import datetime

from api.src.server import app
from shared.supabase import use_client
from shared.settings import settings
from shared.models import LabelPayloadData, LabelSourceEnum, LabelTypeEnum

# Initialize the test client globally
client = TestClient(app)


@pytest.fixture(scope='function')
def test_dataset(auth_token):
	"""Create a temporary test dataset for label testing"""
	# Create test dataset
	with use_client(auth_token) as client:
		dataset_data = {
			'file_name': 'test-labels.tif',
			'file_alias': 'test-labels.tif',
			'file_size': 1000,
			'copy_time': 123,
			'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
			'status': 'uploaded',
		}
		response = client.table(settings.datasets_table).insert(dataset_data).execute()
		dataset_id = response.data[0]['id']

		yield dataset_id

		# Cleanup: Remove test dataset and any associated labels
		client.table(settings.labels_table).delete().eq('dataset_id', dataset_id).execute()
		client.table(settings.datasets_table).delete().eq('id', dataset_id).execute()


@pytest.fixture(scope='function')
def mock_label_file():
	"""Create a mock file for testing label uploads"""
	return io.BytesIO(b'mock label data')


def test_create_label(test_dataset, auth_token):
	"""Test creating a new label for a dataset"""
	# Create a simple polygon for testing
	polygon = Polygon([[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]])

	# Convert to MultiPolygon GeoJSON format
	multipolygon_geojson = {
		'type': 'MultiPolygon',
		'coordinates': [[polygon.exterior.coords[:]]],  # Wrap coords in extra list for MultiPolygon
	}

	# Create a label MultiPolygon (same structure as AOI)
	label_multipolygon = {'type': 'MultiPolygon', 'coordinates': [[[[0, 0], [0, 0.5], [0.5, 0.5], [0.5, 0], [0, 0]]]]}

	# Prepare test label data
	label_payload = LabelPayloadData(
		aoi=multipolygon_geojson,
		label=label_multipolygon,  # Must be a MultiPolygon GeoJSON
		label_source=LabelSourceEnum.visual_interpretation,
		label_quality=1,
		label_type=LabelTypeEnum.segmentation,
	)

	# Make request to create label
	response = client.post(
		f'/datasets/{test_dataset}/labels',
		json=label_payload.model_dump(),
		headers={'Authorization': f'Bearer {auth_token}'},
	)

	# Check response
	assert response.status_code == 200
	data = response.json()

	# Verify the label was correctly saved
	assert data['dataset_id'] == test_dataset
	assert data['label_source'] == label_payload.label_source
	assert data['label_quality'] == label_payload.label_quality
	assert data['label_type'] == label_payload.label_type
	assert 'aoi' in data
	assert 'label' in data


def test_create_label_unauthorized():
	"""Test label creation without authentication"""
	response = client.post(
		'/datasets/1/labels',
		json={},
	)
	assert response.status_code == 401


def test_create_label_invalid_dataset(auth_token):
	"""Test label creation for non-existent dataset"""
	polygon = Polygon([[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]])

	multipolygon_geojson = {'type': 'MultiPolygon', 'coordinates': [[polygon.exterior.coords[:]]]}

	label_multipolygon = {'type': 'MultiPolygon', 'coordinates': [[[[0, 0], [0, 0.5], [0.5, 0.5], [0.5, 0], [0, 0]]]]}

	label_payload = LabelPayloadData(
		aoi=multipolygon_geojson,
		label=label_multipolygon,
		label_source=LabelSourceEnum.visual_interpretation,
		label_quality=1,
		label_type=LabelTypeEnum.segmentation,
	)

	response = client.post(
		'/datasets/99999/labels',  # Non-existent dataset ID
		json=label_payload.model_dump(),
		headers={'Authorization': f'Bearer {auth_token}'},
	)
	assert response.status_code == 404  # Changed from 500 to 404 for non-existent resource


def test_upload_user_labels(test_dataset, auth_token, mock_label_file):
	"""Test uploading user labels"""
	# Prepare form data
	form_data = {
		'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
		'file_type': 'geojson',
		'file_alias': 'test_labels',
		'label_description': 'Test label description',
	}

	files = {'file': ('test_labels.geojson', mock_label_file, 'application/json')}

	response = client.post(
		f'/datasets/{test_dataset}/user-labels',
		data=form_data,
		files=files,
		headers={'Authorization': f'Bearer {auth_token}'},
	)

	assert response.status_code == 200
	data = response.json()

	# Verify the response data
	assert data['dataset_id'] == test_dataset
	assert data['user_id'] == form_data['user_id']
	assert data['file_type'] == form_data['file_type']
	assert data['file_alias'] == form_data['file_alias']
	assert data['label_description'] == form_data['label_description']
	assert data['audited'] == False
	assert 'file_path' in data

	# Cleanup: Remove created file and database entry
	with use_client(auth_token) as supabaseClient:
		supabaseClient.table(settings.label_objects_table).delete().eq('dataset_id', test_dataset).execute()

	if Path(data['file_path']).exists():
		Path(data['file_path']).unlink()


def test_upload_user_labels_unauthorized(test_dataset, mock_label_file):
	"""Test user label upload without authentication"""
	form_data = {
		'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
		'file_type': 'geojson',
		'file_alias': 'test_labels',
		'label_description': 'Test label description',
	}

	files = {'file': ('test_labels.geojson', mock_label_file, 'application/json')}

	response = client.post(f'/datasets/{test_dataset}/user-labels', data=form_data, files=files)
	assert response.status_code == 401


def test_upload_user_labels_invalid_dataset(auth_token, mock_label_file):
	"""Test user label upload for non-existent dataset"""
	form_data = {
		'user_id': '484d53be-2fee-4449-ad36-a6b083aab663',
		'file_type': 'geojson',
		'file_alias': 'test_labels',
		'label_description': 'Test label description',
	}

	files = {'file': ('test_labels.geojson', mock_label_file, 'application/json')}

	response = client.post(
		'/datasets/99999/user-labels',  # Non-existent dataset ID
		data=form_data,
		files=files,
		headers={'Authorization': f'Bearer {auth_token}'},
	)
	assert response.status_code == 404
