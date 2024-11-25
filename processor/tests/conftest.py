import pytest
from pathlib import Path
import tempfile
import shutil

from shared.supabase import login
from shared.settings import settings
from shared.models import Dataset, QueueTask, TaskTypeEnum


@pytest.fixture(scope='session')
def auth_token():
	"""Provide authentication token for tests"""
	return login(settings.processor_username, settings.processor_password)


@pytest.fixture
def test_file():
	"""Fixture to provide test GeoTIFF file path"""
	file_path = Path(__file__).parent / 'test_data' / 'test-data-small.tif'
	if not file_path.exists():
		pytest.skip('Test file not found')
	return file_path


@pytest.fixture
def test_task(test_file):
	"""Create a test task for thumbnail processing"""
	return QueueTask(
		id=1,
		dataset_id=275,
		user_id='484d53be-2fee-4449-ad36-a6b083aab663',
		task_type=TaskTypeEnum.thumbnail,
		priority=1,
		is_processing=False,
		current_position=1,
		estimated_time=0.0,
		build_args={},
	)


@pytest.fixture
def test_dataset(test_file):
	"""Create a test dataset"""
	return Dataset(
		id=1,
		file_name=test_file.name,
		file_alias='test-data.tif',
		user_id='test-user',
		file_size=test_file.stat().st_size,
		copy_time=0.0,
		status='uploaded',
	)
