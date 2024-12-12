from shared.logger import logger

from .doitall import inference_deadwood, transform_mask, extract_bbox
from .upload_prediction import upload_to_supabase
from ..exceptions import ProcessingError


def predict_deadwood(dataset_id, file_path):
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
		raise ProcessingError(
			f'Error uploading to supabase: {res.json()}', task_type='deadwood_segmentation', dataset_id=dataset_id
		)

	logger.info('Inference deadwood Done')
