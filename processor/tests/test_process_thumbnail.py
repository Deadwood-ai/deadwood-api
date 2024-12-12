import pytest

from conftest import DATASET_ID
from shared.supabase import use_client
from shared.settings import settings
from shared.models import TaskTypeEnum, QueueTask
from processor.src.process_thumbnail import process_thumbnail


@pytest.fixture
def thumbnail_task(patch_test_file):
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
