from pathlib import Path
import time

from shared.supabase import use_client, login, verify_token
from shared.settings import settings
from shared.models import StatusEnum, Dataset, QueueTask, Cog
from shared.logger import logger
from shared import monitoring
from .cog.cog import calculate_cog
from .utils import update_status, pull_file_from_storage_server, push_file_to_storage_server
from .exceptions import AuthenticationError, DatasetError, ProcessingError, StorageError


def process_cog(task: QueueTask, temp_dir: Path):
	# login with the processor
	token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)

	user = verify_token(token)
	if not user:
		raise AuthenticationError('Invalid processor token', token=token, task_id=task.id)

	# Load dataset
	try:
		with use_client(token) as client:
			response = client.table(settings.datasets_table).select('*').eq('id', task.dataset_id).execute()
			dataset = Dataset(**response.data[0])
	except Exception as e:
		raise DatasetError(f'Failed to fetch dataset: {str(e)}', dataset_id=task.dataset_id, task_id=task.id)

	# Update status to processing
	update_status(token, dataset_id=dataset.id, status=StatusEnum.cog_processing)

	try:
		# Setup paths
		input_path = Path(temp_dir) / dataset.file_name
		storage_server_file_path = f'{settings.STORAGE_SERVER_DATA_PATH}/archive/{dataset.file_name}'

		# Pull source file
		pull_file_from_storage_server(storage_server_file_path, str(input_path), token)

		# Get options and setup output paths
		options = task.build_args
		cog_folder = Path(dataset.file_name).stem
		file_name = f'{cog_folder}_cog_{options.profile}_ts_{options.tiling_scheme}_q{options.quality}.tif'
		output_path = Path(temp_dir) / file_name

		# Generate COG
		logger.info(f'Calculating COG for dataset {dataset.id} with options: {options}', extra={'token': token})
		t1 = time.time()
		info = calculate_cog(
			str(input_path),
			str(output_path),
			profile=options.profile,
			quality=options.quality,
			skip_recreate=not options.force_recreate,
			tiling_scheme=options.tiling_scheme,
			token=token,
		)
		logger.info(f'COG created for dataset {dataset.id}: {info}', extra={'token': token})

		# Push generated COG
		storage_server_cog_path = f'{settings.STORAGE_SERVER_DATA_PATH}/cogs/{cog_folder}/{file_name}'
		push_file_to_storage_server(str(output_path), storage_server_cog_path, token)
		t2 = time.time()

		# Prepare metadata
		overviews = len(info.IFD) - 1  # since first IFD is the main image
		meta = dict(
			dataset_id=dataset.id,
			cog_folder=cog_folder,
			cog_name=file_name,
			cog_url=f'{cog_folder}/{file_name}',
			cog_size=output_path.stat().st_size,
			runtime=t2 - t1,
			user_id=task.user_id,
			compression=options.profile,
			overviews=overviews,
			tiling_scheme=options.tiling_scheme,
			resolution=int(options.resolution * 100),
			blocksize=info.IFD[0].Blocksize[0],
		)
		cog = Cog(**meta)

	except Exception as e:
		raise ProcessingError(str(e), task_type='cog', task_id=task.id, dataset_id=dataset.id)

	# Save metadata to database
	try:
		# Refresh token before database operation
		token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)
		user = verify_token(token)
		if not user:
			raise AuthenticationError('Token refresh failed', token=token, task_id=task.id)

		with use_client(token) as client:
			send_data = {k: v for k, v in cog.model_dump().items() if v is not None}
			client.table(settings.cogs_table).upsert(send_data).execute()

		# Update final status
		update_status(token, dataset.id, StatusEnum.processed)

	except AuthenticationError:
		raise
	except Exception as e:
		raise DatasetError(f'Failed to save COG metadata: {str(e)}', dataset_id=dataset.id, task_id=task.id)

	# Update monitoring metrics
	# monitoring.cog_counter.inc()
	# monitoring.cog_time.observe(cog.runtime)
	# monitoring.cog_size.observe(cog.cog_size)

	logger.info(
		f'Finished creating new COG for dataset {dataset.id}.',
		extra={'token': token},
	)
