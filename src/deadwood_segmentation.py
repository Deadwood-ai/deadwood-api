import os
from fastapi import HTTPException
from pathlib import Path


from .supabase import use_client, login, verify_token
from .processing import update_status
from .models import QueueTask, Dataset, StatusEnum
from .settings import settings
from .logger import logger


def segment_deadwood(task: QueueTask, token: str):
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

	file_path = Path(settings.tmp_processing_path) / dataset.file_name
	logger.info(
		f'Running deadwood segmentation for dataset {task.dataset_id} with file path {str(file_path)} with command:',
		extra={'token': token},
	)
	logger.info(
		f'bash -c "source /app/deadtreesmodels/venv/bin/activate && python /app/deadtreesmodels/run_deadwood_inference.py --dataset_id={task.dataset_id} --file_path={str(file_path)}"',
		extra={'token': token},
	)
	try:
		update_status(token, dataset_id=dataset.id, status=StatusEnum.deadwood_prediction)
		os.system(
			f'bash -c "source /app/deadtreesmodels/venv/bin/activate && python /app/deadtreesmodels/run_deadwood_inference.py --dataset_id={task.dataset_id} --file_path={str(file_path)}"'
		)
	except Exception as e:
		logger.error(f'Error: {e}', extra={'token': token})
		update_status(token, dataset_id=dataset.id, status=StatusEnum.deadwood_errored)
		return HTTPException(status_code=500, detail='Error running deadwood segmentation')

	logger.info(f'Deadwood segmentation completed for dataset {task.dataset_id}', extra={'token': token})
	update_status(token, dataset_id=dataset.id, status=StatusEnum.processed)
