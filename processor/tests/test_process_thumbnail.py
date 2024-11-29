import pytest
from pathlib import Path
import shutil

from shared.settings import settings
from shared.supabase import use_client
from shared.models import TaskTypeEnum, QueueTask
from processor.src.process_thumbnail import process_thumbnail

DATASET_ID = 275


@pytest.fixture
def test_file():
	"""Fixture to provide test GeoTIFF file path"""
	file_path = Path(__file__).parent / 'test_data' / 'test-data-small.tif'
	if not file_path.exists():
		pytest.skip('Test file not found')
	return file_path


@pytest.fixture
def thumbnail_task(thumbnail_test_file):
	"""Create a test task specifically for thumbnail processing"""
	return QueueTask(
		id=1,
		dataset_id=DATASET_ID,
		user_id='484d53be-2fee-4449-ad36-a6b083aab663',
		task_type=TaskTypeEnum.thumbnail,
		priority=1,
		is_processing=False,
		current_position=1,
		estimated_time=0.0,
		build_args={},
	)


@pytest.fixture()
def thumbnail_test_file(test_file, auth_token):
	"""Fixture to provide test file for thumbnail testing"""
	# Create processing directory if it doesn't exist
	settings.processing_path.mkdir(parents=True, exist_ok=True)

	with use_client(auth_token) as client:
		response = client.table(settings.datasets_table).select('file_name').eq('id', DATASET_ID).execute()
		file_name = response.data[0]['file_name']
	if not response.data:
		pytest.skip('Dataset not found in database')

	# Copy file to processing directory
	dest_path = settings.processing_path / file_name

	shutil.copy2(test_file, dest_path)

	yield test_file

	# Cleanup processing directory after test
	if settings.processing_path.exists():
		shutil.rmtree(settings.processing_path)


def test_process_thumbnail_success(thumbnail_task, auth_token):
	"""Test successful thumbnail processing"""
	process_thumbnail(thumbnail_task, settings.processing_path)

	with use_client(auth_token) as client:
		response = (
			client.table(settings.thumbnails_table).select('*').eq('dataset_id', thumbnail_task.dataset_id).execute()
		)
		assert len(response.data) == 1
		assert response.data[0]['dataset_id'] == thumbnail_task.dataset_id

		# Clean up by removing the test entry
		client.table(settings.thumbnails_table).delete().eq('dataset_id', thumbnail_task.dataset_id).execute()
