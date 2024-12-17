# import pytest

# from conftest import DATASET_ID
# from shared.supabase import use_client
# from shared.settings import settings
# from shared.models import TaskTypeEnum, QueueTask
# from processor.src.process_deadwood_segmentation import process_deadwood_segmentation


# @pytest.fixture
# def deadwood_task(patch_test_file):
# 	"""Create a test task specifically for deadwood segmentation processing"""
# 	return QueueTask(
# 		id=1,
# 		dataset_id=DATASET_ID,
# 		user_id='484d53be-2fee-4449-ad36-a6b083aab663',
# 		task_type=TaskTypeEnum.deadwood_segmentation,
# 		priority=1,
# 		is_processing=False,
# 		current_position=1,
# 		estimated_time=0.0,
# 		build_args={},
# 	)


# @pytest.fixture(autouse=True)
# def cleanup_labels(auth_token, deadwood_task):
# 	"""Fixture to clean up labels after each test"""
# 	yield
	
# 	# Cleanup will run after each test
# 	with use_client(auth_token) as client:
# 		client.table(settings.labels_table).delete().eq('dataset_id', deadwood_task.dataset_id).execute()


# def test_process_deadwood_segmentation_success(deadwood_task, auth_token):
# 	"""Test successful deadwood segmentation processing"""
# 	process_deadwood_segmentation(deadwood_task, auth_token, settings.processing_path)

# 	with use_client(auth_token) as client:
# 		response = client.table(settings.labels_table).select('*').eq('dataset_id', deadwood_task.dataset_id).execute()
# 		data = response.data[0]
# 		assert len(response.data) == 1
# 		assert 'aoi' in data
# 		assert 'label' in data
# 		assert data['dataset_id'] == deadwood_task.dataset_id
