from typing import Annotated
import uuid
from pathlib import Path
import time
import hashlib
import rasterio
from rasterio.env import Env
from concurrent.futures import ProcessPoolExecutor
import asyncio
from datetime import datetime

from fastapi import APIRouter, UploadFile, Depends, HTTPException, Form
from fastapi.security import OAuth2PasswordBearer


from ..models import Metadata, MetadataPayloadData, Dataset, StatusEnum, UserLabelObject
from ..supabase import use_client, verify_token
from ..settings import settings
from ..logger import logger
from ..deadwood.osm import get_admin_tags
from .. import monitoring

# Add this import at the top of the file
import tempfile
import shutil

# create the router for the upload
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')

executor = ProcessPoolExecutor(max_workers=5)


# little helper
def format_size(size: int) -> str:
	"""Converting the filesize of the geotiff into a human readable format for the logger

	Args:
	    size (int): File size in bytes

	Returns:
	    str: A proper human readable size string in bytes, KB, MB or GB
	"""
	if size < 1024:
		return f'{size} bytes'
	elif size < 1024**2:
		return f'{size / 1024:.2f} KB'
	elif size < 1024**3:
		return f'{size / 1024**2:.2f} MB'
	else:
		return f'{size / 1024**3:.2f} GB'


def combine_chunks(
	tmp_dir: Path,
	total_chunks: int,
	filename: str,
	target_path: Path,
	token: str,
	initial_dataset: Dataset,
) -> None:
	"""Combine all chunks into a single file and process it."""
	logger.info(f'Combining chunks for file {filename}', extra={'token': token})
	combined_file = tmp_dir / filename
	with combined_file.open('wb') as outfile:
		for i in range(int(total_chunks)):
			chunk_file = tmp_dir / f'chunk_{i}'
			with chunk_file.open('rb') as infile:
				outfile.write(infile.read())
			chunk_file.unlink()  # Remove the chunk file after combining

	logger.info(f'Combined chunks for file {filename}', extra={'token': token})
	# Move the combined file to the target path
	shutil.move(str(combined_file), str(target_path))

	logger.info(f'Moved combined file to target path {target_path}', extra={'token': token})

	# Compute SHA256 checksum
	sha256 = compute_sha256(target_path)

	logger.info(f'Computed SHA256 checksum {sha256} for file {target_path}', extra={'token': token})

	# Open with rasterio and get bounds
	with Env(GTIFF_SRS_SOURCE='EPSG'):
		with rasterio.open(str(target_path), 'r') as src:
			try:
				transformed_bounds = rasterio.warp.transform_bounds(src.crs, 'EPSG:4326', *src.bounds)
			except Exception as e:
				logger.error(f'No CRS found for {target_path}: {e}')
				return

	logger.info(f'Transformed bounds {transformed_bounds} for file {target_path}', extra={'token': token})

	# Update dataset entry
	update_dataset_entry(initial_dataset.id, target_path, sha256, transformed_bounds, token)

	# Update the metadata admin level
	update_metadata_admin_level(initial_dataset.id, token)

	logger.info(f'Updated dataset entry {initial_dataset} for file {target_path}', extra={'token': token})

	# Clean up the temporary directory
	shutil.rmtree(tmp_dir)

	logger.info(f'Cleaned up temporary directory {tmp_dir}', extra={'token': token})


async def run_combine_chunks_and_shutdown_executor(
	tmp_dir,
	total_chunks,
	filename,
	target_path,
	token,
	initial_dataset,
):
	try:
		# Create a new executor
		with ProcessPoolExecutor(max_workers=5) as executor:
			loop = asyncio.get_running_loop()
			await loop.run_in_executor(
				executor,
				combine_chunks,
				tmp_dir,
				total_chunks,
				filename,
				target_path,
				token,
				initial_dataset,
			)
		# Executor is automatically shut down when exiting the 'with' block
	except Exception as e:
		# Log any exceptions from combine_chunks
		logger.exception(f'Error in combine_chunks: {e}', extra={'token': token})


def compute_sha256(file_path: Path) -> str:
	"""Compute SHA256 checksum of a file."""
	sha256_hash = hashlib.sha256()
	with file_path.open('rb') as f:
		for byte_block in iter(lambda: f.read(4096), b''):
			sha256_hash.update(byte_block)
	return sha256_hash.hexdigest()


def create_initial_dataset_entry(filename: str, file_alias: str, user_id: str, copy_time: int, token: str) -> Dataset:
	"""Create an initial dataset entry with available information."""
	data = dict(
		file_name=filename,
		file_alias=file_alias,
		status=StatusEnum.uploading,
		user_id=user_id,
		copy_time=copy_time,
		file_size=None,
		sha256=None,
		bbox=None,
	)
	dataset = Dataset(**data)

	with use_client(token) as client:
		try:
			send_data = {k: v for k, v in dataset.model_dump().items() if k != 'id' and v is not None}
			response = client.table(settings.datasets_table).insert(send_data).execute()
		except Exception as e:
			logger.exception(f'Error creating initial dataset entry: {str(e)}', extra={'token': token})
			raise HTTPException(status_code=400, detail=f'Error creating initial dataset entry: {str(e)}')

	return Dataset(**response.data[0])


def update_dataset_entry(dataset_id: int, target_path: Path, sha256: str, bounds, token: str):
	"""Update the existing dataset entry with processed information."""

	data = dict(
		file_name=None,
		file_alias=None,
		copy_time=None,
		user_id=None,
		file_size=target_path.stat().st_size,
		sha256=sha256,
		bbox=bounds,
		status=StatusEnum.uploaded,
	)
	dataset_update = Dataset(**data)

	with use_client(token) as client:
		try:
			client.table(settings.datasets_table).update(
				dataset_update.model_dump(include={'file_size', 'sha256', 'bbox', 'status'})
			).eq('id', dataset_id).execute()
		except Exception as e:
			logger.exception(f'Error updating dataset: {str(e)}', extra={'token': token})
			raise HTTPException(status_code=400, detail=f'Error updating dataset: {str(e)}')


def update_metadata_admin_level(dataset_id: int, token: str):
	"""
	Update the admin level information in the metadata table for a given dataset.

	Args:
		dataset_id (int): The ID of the dataset.
		token (str): The authentication token.
	"""
	try:
		# Calculate the centroid of the bounding box
		with use_client(token) as client:
			response = client.table(settings.datasets_table).select('*').eq('id', dataset_id).execute()
			data = Dataset(**response.data[0])
	except Exception as e:
		logger.exception(f'Error getting dataset {dataset_id}: {str(e)}', extra={'token': token})
		raise HTTPException(status_code=400, detail=f'Error getting dataset {dataset_id}: {str(e)}')

	# Get the admin tags for the centroid
	try:
		(lvl1, lvl2, lvl3) = get_admin_tags(data.centroid)
	except Exception as e:
		logger.exception(f'Error getting admin tags for dataset {dataset_id}: {str(e)}', extra={'token': token})
		raise HTTPException(status_code=400, detail=f'Error getting admin tags for dataset {dataset_id}: {str(e)}')

	try:
		with use_client(token) as client:
			# Update the metadata in the database
			metadata_update = {
				'admin_level_1': lvl1,
				'admin_level_2': lvl2,
				'admin_level_3': lvl3,
			}
			client.table(settings.metadata_table).update(metadata_update).eq('dataset_id', dataset_id).execute()

	except Exception as e:
		logger.error(
			f'An error occurred while updating admin level information for dataset_id {dataset_id}: {str(e)}',
			extra={'token': token, 'dataset_id': dataset_id},
		)
	logger.info(
		f'Updated admin level information for Dataset {dataset_id}',
		extra={'token': token, 'dataset_id': dataset_id},
	)
	return True


@router.post('/datasets/chunk')
async def upload_geotiff_chunk(
	file: UploadFile,
	chunk_index: Annotated[int, Form()],
	chunks_total: Annotated[int, Form()],
	filename: Annotated[str, Form()],
	copy_time: Annotated[int, Form()],
	upload_id: Annotated[str, Form()],
	token: Annotated[str, Depends(oauth2_scheme)],
):
	"""
	Handle chunked upload of a GeoTIFF file.

	This endpoint receives chunks of a file, saves them temporarily,
	and combines them when all chunks are received.
	"""
	# Verify the token
	user = verify_token(token)
	if not user:
		raise HTTPException(status_code=401, detail='Invalid token')

	# Create a temporary directory using the upload_id
	tmp_dir = Path(tempfile.gettempdir()) / upload_id
	tmp_dir.mkdir(parents=True, exist_ok=True)

	logger.info(f'Received chunk {chunk_index} of {chunks_total} for upload {upload_id}', extra={'token': token})

	# Save the chunk
	chunk_file = tmp_dir / f'chunk_{chunk_index}'
	with chunk_file.open('wb') as buffer:
		content = await file.read()
		buffer.write(content)

	logger.info(f'Saved chunk {chunk_index} of {chunks_total} for upload {upload_id}', extra={'token': token})

	# If this is the last chunk, start the combination process
	if int(chunk_index) == int(chunks_total) - 1:
		uid = str(uuid.uuid4())
		file_name = f'{uid}_{Path(filename).stem}.tif'
		target_path = settings.archive_path / file_name
		file_alias = file.filename

		initial_dataset = create_initial_dataset_entry(file_name, file_alias, user.id, copy_time, token)

		# Schedule the async function as a background task
		asyncio.create_task(
			run_combine_chunks_and_shutdown_executor(
				tmp_dir,
				chunks_total,
				file_name,
				target_path,
				token,
				initial_dataset,
			)
		)

		return initial_dataset

	return {'message': f'Chunk {chunk_index} of {chunks_total} received'}


@router.post('/datasets/{dataset_id}/label-object')
async def upload_label_object(
	file: UploadFile,
	dataset_id: int,
	user_id: Annotated[str, Form()],
	file_type: Annotated[str, Form()],
	file_alias: Annotated[str, Form()],
	label_description: Annotated[str, Form()],
	token: Annotated[str, Depends(oauth2_scheme)],
):
	"""
	Upload a label object.
	"""

	user = verify_token(token)
	if not user:
		raise HTTPException(status_code=401, detail='Invalid token')
	logger.info(f'Received label object for dataset {dataset_id} from user {user_id}', extra={'token': token})

	# create folder if not exists settings.labels_objects_path / dataset_id
	if not (settings.label_objects_path / str(dataset_id)).exists():
		(settings.label_objects_path / str(dataset_id)).mkdir(parents=True, exist_ok=True)
	# count number of files in the folder

	target_path = (
		settings.label_objects_path
		/ str(dataset_id)
		/ f'{file_alias}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.{file_type}'
	)

	try:
		with target_path.open('wb') as buffer:
			buffer.write(await file.read())
	except Exception as e:
		logger.exception(f'Error saving label object to {target_path}: {str(e)}', extra={'token': token})
		raise HTTPException(status_code=400, detail=f'Error saving label object to {target_path}: {str(e)}')

	logger.info(f'Saved label object to {target_path}', extra={'token': token})

	# insert the label object into the database
	label_object = UserLabelObject(
		dataset_id=dataset_id,
		user_id=user_id,
		file_type=file_type,
		file_alias=file_alias,
		file_path=str(target_path),
		label_description=label_description,
		audited=False,
	)

	try:
		with use_client(token) as client:
			send_data = {k: v for k, v in label_object.model_dump().items() if v is not None}
			print('send_data:', send_data)
			print('table:', settings.label_objects_table)
			response = client.table(settings.label_objects_table).insert(send_data).execute()
			logger.info(
				f'Inserted label object into database: {response.data[0]}',
				extra={'token': token, 'dataset_id': dataset_id, 'user_id': user_id},
			)
	except Exception as e:
		logger.exception(f'Error inserting label object into database: {str(e)}', extra={'token': token})
		raise HTTPException(status_code=400, detail=f'Error inserting label object into database: {str(e)}')

	return label_object


# Main routes for the logic
@router.post('/datasets')
async def upload_geotiff(file: UploadFile, token: Annotated[str, Depends(oauth2_scheme)]):
	"""
	Create a new Dataset by uploading a GeoTIFF file.

	Further metadata is not yet necessary. The response will contain a Dataset.id
	that is needed for subsequent calls to the API. Once, the GeoTIFF is uploaded,
	the backend will start pre-processing the file.
	It can only be used in the front-end once preprocessing finished AND all mandatory
	metadata is set.

	To send the file use the `multipart/form-data` content type. The file has to be sent as the
	value of a field named `file`. For example, using HTML forms like this:

	```html
	<form action="/upload" method="post" enctype="multipart/form-data">
	    <input type="file" name="file">
	    <input type="submit">
	</form>
	```

	Or using the `requests` library in Python like this:

	```python
	import requests
	url = "http://localhost:8000/upload"
	files = {"file": open("example.txt", "rb")}
	response = requests.post(url, files=files)
	print(response.json())
	```

	"""
	# count an invoke
	monitoring.uploads_invoked.inc()

	# first thing we do is verify the token
	user = verify_token(token)
	if not user:
		return HTTPException(status_code=401, detail='Invalid token')

	# we create a uuid for this dataset
	uid = str(uuid.uuid4())

	# new file name
	file_name = f'{uid}_{Path(file.filename).stem}.tif'

	# use the settings path to figure out a new location for this file
	target_path = settings.archive_path / file_name

	# start a timer
	t1 = time.time()

	# save the file
	with target_path.open('wb') as buffer:
		buffer.write(await file.read())

	# create the checksum
	with target_path.open('rb') as f:
		sha256 = hashlib.sha256(f.read()).hexdigest()

	# try to open with rasterio
	with rasterio.open(str(target_path), 'r') as src:
		bounds = src.bounds
		transformed_bounds = rasterio.warp.transform_bounds(src.crs, 'EPSG:4326', *bounds)

	# stop the timer
	t2 = time.time()

	# fill the metadata
	# dataset = Dataset(
	data = dict(
		file_name=target_path.name,
		file_alias=file.filename,
		file_size=target_path.stat().st_size,
		copy_time=t2 - t1,
		sha256=sha256,
		bbox=transformed_bounds,
		status=StatusEnum.pending,
		user_id=user.id,
	)
	# print(data)
	dataset = Dataset(**data)

	# upload the dataset
	with use_client(token) as client:
		try:
			send_data = {k: v for k, v in dataset.model_dump().items() if k != 'id' and v is not None}
			response = client.table(settings.datasets_table).insert(send_data).execute()
		except Exception as e:
			logger.exception(
				f'An error occurred while trying to upload the dataset: {str(e)}',
				extra={'token': token, 'user_id': user.id},
			)
			raise HTTPException(
				status_code=400,
				detail=f'An error occurred while trying to upload the dataset: {str(e)}',
			)

	# update the dataset with the id
	dataset = Dataset(**response.data[0])

	# do some monitoring
	monitoring.uploads_counter.inc()
	monitoring.upload_time.observe(dataset.copy_time)
	monitoring.upload_size.observe(dataset.file_size)

	logger.info(
		f'Created new dataset <ID={dataset.id}> with file {dataset.file_alias}. ({format_size(dataset.file_size)}). Took {dataset.copy_time:.2f}s.',
		extra={'token': token, 'user_id': user.id, 'dataset_id': dataset.id},
	)

	return dataset


@router.put('/datasets/{dataset_id}/metadata')
def upsert_metadata(
	dataset_id: int,
	payload: MetadataPayloadData,
	token: Annotated[str, Depends(oauth2_scheme)],
):
	"""
	Insert or Update the metadata of a Dataset.

	Right now, the API requires that always a valid Metadata instance is sent.
	Thus, the frontend can change the values and send the whole Metadata object.
	The token needs to include the access token of the user that is allowed to change the metadata.

	"""
	# count an invoke
	monitoring.metadata_invoked.inc()

	# first thing we do is verify the token
	user = verify_token(token)
	if not user:
		return HTTPException(status_code=401, detail='Invalid token')

	logger.info(
		f'Upserting metadata for Dataset {dataset_id}',
		extra={'token': token, 'dataset_id': dataset_id, 'user_id': user.id},
	)
	# load the metadata info - if it already exists in the database

	try:
		with use_client(token) as client:
			response = client.table(settings.metadata_table).select('*').eq('dataset_id', dataset_id).execute()
			if len(response.data) > 0:
				metadata = Metadata(**response.data[0]).model_dump()
			else:
				logger.info(
					f'No existing Metadata found for Dataset {dataset_id}. Creating a new one.',
					extra={'token': token, 'dataset_id': dataset_id, 'user_id': user.id},
				)
				metadata = {'dataset_id': dataset_id, 'user_id': user.id}
	except Exception as e:
		msg = f'An error occurred while trying to get the metadata of Dataset <ID={dataset_id}>: {str(e)}'
		logger.exception(msg, extra={'token': token, 'dataset_id': dataset_id, 'user_id': user.id})
		return HTTPException(status_code=400, detail=msg)

	# update the given metadata if any with the payload
	try:
		metadata.update(**{k: v for k, v in payload.model_dump().items() if v is not None})
		metadata = Metadata(**metadata)
	except Exception as e:
		msg = f'An error occurred while trying to create the updated metadata: {str(e)}'

		logger.exception(msg, extra={'token': token, 'dataset_id': dataset_id, 'user_id': user.id})
		return HTTPException(status_code=400, detail=msg)

	try:
		# upsert the given metadata entry with the merged data
		with use_client(token) as client:
			send_data = {k: v for k, v in metadata.model_dump().items() if v is not None}
			response = client.table(settings.metadata_table).upsert(send_data).execute()
	except Exception as e:
		err_msg = f'An error occurred while trying to upsert the metadata of Dataset <ID={dataset_id}>: {e}'

		# log the error to the database
		logger.error(
			err_msg,
			extra={'token': token, 'dataset_id': dataset_id, 'user_id': user.id},
		)

		# return a response with the error message
		return HTTPException(status_code=400, detail=err_msg)

	# no error occured, so return the upserted metadata
	logger.info(
		f'Upserted metadata for Dataset {dataset_id}. Upsert payload provided by user: {payload}',
		extra={'token': token, 'dataset_id': dataset_id, 'user_id': user.id},
	)

	# update the metadata
	metadata = Metadata(**response.data[0])
	monitoring.metadata_counter.inc()

	return metadata
