from pathlib import Path
import time
from fastapi import HTTPException

from ...shared.supabase import use_client, login, verify_token
from ...shared.settings import settings
from ...shared.models import StatusEnum, Dataset, QueueTask, Cog
from ...shared.logger import logger
from ...shared import monitoring
from .cog.cog import calculate_cog
from .utils import update_status, pull_file_from_storage_server, push_file_to_storage_server


def process_cog(task: QueueTask, temp_dir: Path):
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

	# update the status to processing
	update_status(token, dataset_id=dataset.id, status=StatusEnum.cog_processing)

	# get local file path
	input_path = Path(temp_dir) / dataset.file_name
	# get the remote file path
	storage_server_file_path = f'{settings.storage_server_data_path}/archive/{dataset.file_name}'

	pull_file_from_storage_server(storage_server_file_path, str(input_path), token)

	# get the options
	options = task.build_args

	# get the output settings
	cog_folder = Path(dataset.file_name).stem
	file_name = f'{cog_folder}_cog_{options.profile}_ts_{options.tiling_scheme}_q{options.quality}.tif'

	# output path is in the temporary directory
	output_path = Path(temp_dir) / file_name
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
	logger.info(
		f'COG created for dataset {dataset.id}: {info}',
		extra={'token': token},
	)
	# push the file to the storage server
	storage_server_cog_path = f'{settings.storage_server_data_path}/cogs/{cog_folder}/{file_name}'

	push_file_to_storage_server(str(output_path), storage_server_cog_path, token)

	t2 = time.time()

	# calculate number of overviews
	overviews = len(info.IFD) - 1  # since first IFD is the main image

	# fill the metadata
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

	# save the metadata to the database
	cog = Cog(**meta)

	# check if session is still active and token is valid
	token = login(settings.processor_username, settings.processor_password)
	user = verify_token(token)
	if not user:
		return HTTPException(status_code=401, detail='Invalid token')

	try:
		with use_client(token) as client:
			send_data = {k: v for k, v in cog.model_dump().items() if v is not None}
			client.table(settings.cogs_table).upsert(send_data).execute()

		# If we reach here, processing was successfu    l
		update_status(token, dataset.id, StatusEnum.processed)
	except Exception as e:
		logger.error(f'Error: {e}')
		return HTTPException(status_code=500, detail='Error saving cog to database')

	# monitoring
	monitoring.cog_counter.inc()
	monitoring.cog_time.observe(cog.runtime)
	monitoring.cog_size.observe(cog.cog_size)

	logger.info(
		f'Finished creating new COG for dataset {dataset.id}.',
		extra={'token': token},
	)
