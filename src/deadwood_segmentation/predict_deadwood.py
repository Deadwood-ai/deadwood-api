import os
from fastapi import HTTPException
from pathlib import Path


from ..supabase import use_client, login, verify_token
from ..processing import update_status, pull_file_from_storage_server
from ..models import QueueTask, Dataset, StatusEnum
from ..settings import settings
from ..logger import logger

from .doitall import inference_deadwood, transform_mask, extract_bbox
from .upload_prediction import upload_to_supabase


def run_deadwood_inference(dataset_id, file_path):
	# try:
	logger.info('Running deadwood inference')
	polygons = inference_deadwood(file_path)

	logger.info('Transforming polygons')
	transformed_polygons = transform_mask(polygons, file_path)

	logger.info('Extracting bbox')
	bbox_geojson = extract_bbox(file_path)

	# logging.info("Uploading to supabase")
	res = upload_to_supabase(
		dataset_id,
		transformed_polygons,
		bbox_geojson,
		'segmentation',
		'model_prediction',
		3,
	)
	if res.status_code == 200:
		logger.info('Uploaded to supabase')
	else:
		logger.error(f'Error uploading to supabase: {res.json()}')
		return HTTPException(status_code=500, detail='Error uploading to supabase')

	logger.info('Inference deadwood Done')


def segment_deadwood(task: QueueTask, token: str, temp_dir: Path):
	## run the script:
	## source /home/jj1049/deadtreesmodels/venv/bin/activate &&
	## python /home/jj1049/deadtreesmodels/deadwood_segmentation.py --dataset_id <dataset_id> --file_path <file_path>
	## get the file path from the task
	# login with the processor
	token = login(settings.processor_username, settings.processor_password)

	user = verify_token(token)
	if not user:
		return HTTPException(status_code=401, detail='Invalid token')

	try:
		with use_client(token) as client:
			response = client.table(settings.datasets_table).select('*').eq('id', task.dataset_id).execute()
			dataset = Dataset(**response.data[0])
	except Exception as e:
		logger.error(f'Error: {e}')
		return HTTPException(status_code=500, detail='Error fetching dataset')

	# update_status(token, dataset_id=dataset.id, status=StatusEnum.deadwood_prediction)
	update_status(token, dataset_id=dataset.id, status=StatusEnum.processing)

	# get local file path
	file_path = Path(temp_dir) / dataset.file_name
	# get the remote file path
	storage_server_file_path = f'{settings.storage_server_data_path}/archive/{dataset.file_name}'
	pull_file_from_storage_server(storage_server_file_path, str(file_path), token)

	# logger.info(
	# 	f'bash -c "source /app/deadtreesmodels/venv/bin/activate && python /app/deadtreesmodels/run_deadwood_inference.py --dataset_id={task.dataset_id} --file_path={str(file_path)}"',
	# 	extra={'token': token},
	# )
	try:
		logger.info(
			f'Running deadwood segmentation for dataset {task.dataset_id} with file path {str(file_path)} with command:',
			extra={'token': token},
		)
		# os.system(
		# 	f'bash -c "source /app/deadtreesmodels/venv/bin/activate && python /app/deadtreesmodels/run_deadwood_inference.py --dataset_id={task.dataset_id} --file_path={str(file_path)}"'
		# )
		run_deadwood_inference(task.dataset_id, file_path)
	except Exception as e:
		logger.error(f'Error: {e}', extra={'token': token})
		# update_status(token, dataset_id=dataset.id, status=StatusEnum.deadwood_errored)
		update_status(token, dataset_id=dataset.id, status=StatusEnum.errored)
		return HTTPException(status_code=500, detail='Error running deadwood segmentation')

	logger.info(f'Deadwood segmentation completed for dataset {task.dataset_id}', extra={'token': token})
	update_status(token, dataset_id=dataset.id, status=StatusEnum.processed)
