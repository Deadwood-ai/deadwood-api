from pathlib import Path
import time
import paramiko
import os

from fastapi import HTTPException

from .supabase import use_client, login, verify_token
from .settings import settings
from .models import StatusEnum, Dataset, QueueTask, Cog, Thumbnail
from .logger import logger
from . import monitoring
from .deadwood.cog import calculate_cog
from .deadwood.thumbnail import calculate_thumbnail


def pull_file_from_storage_server(remote_file_path: str, local_file_path: str, token: str):
	# Check if the file already exists locally
	if os.path.exists(local_file_path):
		logger.info(f'File already exists locally at: {local_file_path}')
		return

	with paramiko.SSHClient() as ssh:
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		pkey = paramiko.RSAKey.from_private_key_file(
			settings.ssh_private_key_path, password=settings.ssh_private_key_passphrase
		)
		logger.info(
			f'Connecting to storage server: {settings.storage_server_ip} as {settings.storage_server_username}',
			extra={'token': token},
		)

		ssh.connect(
			hostname=settings.storage_server_ip,
			username=settings.storage_server_username,
			pkey=pkey,
			port=22,  # Add this line to specify the default SSH port
		)

		with ssh.open_sftp() as sftp:
			logger.info(
				f'Pulling file from storage server: {remote_file_path} to {local_file_path}', extra={'token': token}
			)

			# Create the directory for local_file_path if it doesn't exist
			local_dir = Path(local_file_path).parent
			local_dir.mkdir(parents=True, exist_ok=True)
			sftp.get(remote_file_path, local_file_path)

		# Check if the file exists after pulling
		if os.path.exists(local_file_path):
			logger.info(f'File successfully saved at: {local_file_path}', extra={'token': token})
		else:
			logger.error(f'Error: File not found at {local_file_path} after pulling', extra={'token': token})


def push_file_to_storage_server(local_file_path: str, remote_file_path: str, token: str):
	with paramiko.SSHClient() as ssh:
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		logger.info(
			f'Connecting to storage server: {settings.storage_server_ip} as {settings.storage_server_username}',
			extra={'token': token},
		)
		pkey = paramiko.RSAKey.from_private_key_file(
			settings.ssh_private_key_path, password=settings.ssh_private_key_passphrase
		)
		ssh.connect(
			hostname=settings.storage_server_ip,
			username=settings.storage_server_username,
			pkey=pkey,
			port=22,  # Add this line to specify the default SSH port
		)
		with ssh.open_sftp() as sftp:
			logger.info(
				f'Pushing file to storage server: {local_file_path} to {remote_file_path}', extra={'token': token}
			)

			# Extract the remote directory path
			remote_dir = os.path.dirname(remote_file_path)

			try:
				sftp.stat(remote_file_path)
				logger.warning(
					f'File {remote_file_path} already exists and will be overwritten', extra={'token': token}
				)
			except IOError:
				logger.info(f'No existing file found at {remote_file_path}', extra={'token': token})

			# Ensure the remote directory exists
			try:
				sftp.stat(remote_dir)
			except IOError:
				try:
					sftp.mkdir(remote_dir)
					logger.info(f'Created directory {remote_dir}', extra={'token': token})
				except IOError as e:
					logger.warning(f'Error creating directory {remote_dir}: {e}', extra={'token': token})

			# Push the file
			try:
				sftp.put(local_file_path, remote_file_path)
				logger.info(f'File successfully pushed to: {remote_file_path}', extra={'token': token})
			except IOError as e:
				logger.error(f'Failed to push file to {remote_file_path}: {str(e)}', extra={'token': token})
				raise


def update_status(token: str, dataset_id: int, status: StatusEnum):
	"""Function to update the status field of a dataset about the cog calculation process.

	Args:
	    token (str): Supabase client session token
	    dataset_id (int): Unique id of the dataset
	    status (StatusEnum): The current status of the cog calculation process to set the dataset to
	"""
	with use_client(token) as client:
		client.table(settings.datasets_table).update(
			{
				'status': status.value,
			}
		).eq('id', dataset_id).execute()


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


def process_thumbnail(task: QueueTask, temp_dir: Path):
	# login with the processor
	token = login(settings.processor_username, settings.processor_password)

	user = verify_token(token)
	if not user:
		return HTTPException(status_code=401, detail='Invalid token')

	# load the dataset
	try:
		with use_client(token) as client:
			response = client.table(settings.datasets_table).select('*').eq('id', task.dataset_id).execute()
			dataset = Dataset(**response.data[0])
	except Exception as e:
		logger.error(f'Error: {e}')
		return HTTPException(status_code=500, detail='Error fetching dataset')

	# update the status to processing
	update_status(token, dataset_id=dataset.id, status=StatusEnum.thumbnail_processing)

	# get local file paths
	input_path = Path(temp_dir) / dataset.file_name
	thumbnail_file_name = dataset.file_name.replace('.tif', '.jpg')
	output_path = Path(temp_dir) / thumbnail_file_name

	# get the remote file path
	storage_server_file_path = f'{settings.storage_server_data_path}/archive/{dataset.file_name}'
	pull_file_from_storage_server(storage_server_file_path, str(input_path), token)

	t1 = time.time()
	calculate_thumbnail(str(input_path), str(output_path))
	logger.info(
		f'Thumbnail generated for dataset {dataset.id}',
		extra={'token': token},
	)

	# push the file to the storage server
	storage_server_thumbnail_path = f'{settings.storage_server_data_path}/thumbnails/{thumbnail_file_name}'
	push_file_to_storage_server(str(output_path), storage_server_thumbnail_path, token)

	t2 = time.time()

	# fill the metadata
	meta = dict(
		dataset_id=dataset.id,
		user_id=task.user_id,
		thumbnail_path=thumbnail_file_name,
		runtime=t2 - t1,
	)

	thumbnail = Thumbnail(**meta)

	try:
		with use_client(token) as client:
			# Use upsert instead of delete and insert
			client.table(settings.thumbnail_table).upsert(
				thumbnail.model_dump(),
				on_conflict='dataset_id',  # Assuming dataset_id is the primary key
			).execute()
	except Exception as e:
		logger.error(f'Error: {e}')
		return HTTPException(status_code=500, detail='Error saving thumbnail to database')

	# If we reach here, processing was successful
	update_status(token, dataset.id, StatusEnum.processed)

	logger.info(
		f'Finished creating thumbnail for dataset {dataset.id}.',
		extra={'token': token},
	)
