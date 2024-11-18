from typing import Annotated
from pathlib import Path
import hashlib
import uuid
import time
import aiofiles
from fastapi import UploadFile, Depends, HTTPException, Form, APIRouter
from fastapi.security import OAuth2PasswordBearer

from shared.models import Metadata, MetadataPayloadData
from shared.supabase import use_client, verify_token
from shared.settings import settings
from shared.logger import logger
from shared import monitoring

from ..upload import create_initial_dataset_entry, get_transformed_bounds, get_file_identifier

router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')


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
	"""Handle chunked upload of a GeoTIFF file with incremental hash computation"""
	user = verify_token(token)
	if not user:
		raise HTTPException(status_code=401, detail='Invalid token')

	chunk_index = int(chunk_index)
	chunks_total = int(chunks_total)

	upload_file_name = f'{upload_id}.tif'
	upload_target_path = settings.archive_path / upload_file_name

	# Write chunk and update hash
	content = await file.read()
	mode = 'wb' if chunk_index == 0 else 'ab'
	async with aiofiles.open(upload_target_path, mode) as buffer:
		await buffer.write(content)

	# Process final chunk
	if chunk_index == chunks_total - 1:
		try:
			# rename file
			uid = str(uuid.uuid4())
			file_name = f'{uid}_{Path(filename).stem}.tif'
			target_path = settings.archive_path / file_name
			upload_target_path.rename(target_path)

			# Get final hash
			final_sha256 = get_file_identifier(target_path)
			# logger.info(
			# 	f'Hashing took {hash_time:.2f}s for {target_path.stat().st_size / 1024 / 1024 / 1024:.2f} GB',
			# 	extra={'token': token},
			# )

			# Get bounds
			bbox = get_transformed_bounds(target_path)

			# Update dataset entry
			dataset = create_initial_dataset_entry(
				filename=str(target_path),
				file_alias=filename,
				user_id=user.id,
				copy_time=copy_time,
				token=token,
				file_size=target_path.stat().st_size,
				bbox=bbox,
				sha256=final_sha256,
			)

			return dataset

		except Exception as e:
			logger.exception(f'Error processing final chunk: {e}', extra={'token': token})
			raise HTTPException(status_code=500, detail=str(e))

	return {'message': f'Chunk {chunk_index} of {chunks_total} received'}


# Main routes for the logic
# @router.post('/datasets')
# async def upload_geotiff(file: UploadFile, token: Annotated[str, Depends(oauth2_scheme)]):
# 	"""
# 	Create a new Dataset by uploading a GeoTIFF file.

# 	Further metadata is not yet necessary. The response will contain a Dataset.id
# 	that is needed for subsequent calls to the API. Once, the GeoTIFF is uploaded,
# 	the backend will start pre-processing the file.
# 	It can only be used in the front-end once preprocessing finished AND all mandatory
# 	metadata is set.

# 	To send the file use the `multipart/form-data` content type. The file has to be sent as the
# 	value of a field named `file`. For example, using HTML forms like this:

# 	```html
# 	<form action="/upload" method="post" enctype="multipart/form-data">
# 	    <input type="file" name="file">
# 	    <input type="submit">
# 	</form>
# 	```

# 	Or using the `requests` library in Python like this:

# 	```python
# 	import requests
# 	url = "http://localhost:8000/upload"
# 	files = {"file": open("example.txt", "rb")}
# 	response = requests.post(url, files=files)
# 	print(response.json())
# 	```

# 	"""
# 	# count an invoke
# 	monitoring.uploads_invoked.inc()

# 	# first thing we do is verify the token
# 	user = verify_token(token)
# 	if not user:
# 		return HTTPException(status_code=401, detail='Invalid token')

# 	# we create a uuid for this dataset
# 	uid = str(uuid.uuid4())

# 	# new file name
# 	file_name = f'{uid}_{Path(file.filename).stem}.tif'

# 	# use the settings path to figure out a new location for this file
# 	target_path = settings.archive_path / file_name

# 	# start a timer
# 	t1 = time.time()

# 	# Stream the file in chunks instead of loading it all at once
# 	sha256_hash = hashlib.sha256()
# 	chunk_size = 4 * 1024 * 1024  # 4MB chunks for better performance with large files

# 	try:
# 		with target_path.open('wb') as buffer:
# 			while chunk := await file.read(chunk_size):
# 				buffer.write(chunk)
# 				sha256_hash.update(chunk)

# 		sha256 = sha256_hash.hexdigest()
# 	except Exception as e:
# 		logger.exception(f'Error saving file: {str(e)}', extra={'token': token})
# 		raise HTTPException(status_code=400, detail=f'Error saving file: {str(e)}')

# 	# try to open with rasterio
# 	with rasterio.open(str(target_path), 'r') as src:
# 		bounds = src.bounds
# 		transformed_bounds = rasterio.warp.transform_bounds(src.crs, 'EPSG:4326', *bounds)

# 	# stop the timer
# 	t2 = time.time()

# 	# fill the metadata
# 	# dataset = Dataset(
# 	data = dict(
# 		file_name=target_path.name,
# 		file_alias=file.filename,
# 		file_size=target_path.stat().st_size,
# 		copy_time=t2 - t1,
# 		sha256=sha256,
# 		bbox=transformed_bounds,
# 		status=StatusEnum.pending,
# 		user_id=user.id,
# 	)
# 	# print(data)
# 	dataset = Dataset(**data)

# 	# upload the dataset
# 	with use_client(token) as client:
# 		try:
# 			send_data = {k: v for k, v in dataset.model_dump().items() if k != 'id' and v is not None}
# 			response = client.table(settings.datasets_table).insert(send_data).execute()
# 		except Exception as e:
# 			logger.exception(
# 				f'An error occurred while trying to upload the dataset: {str(e)}',
# 				extra={'token': token, 'user_id': user.id},
# 			)
# 			raise HTTPException(
# 				status_code=400,
# 				detail=f'An error occurred while trying to upload the dataset: {str(e)}',
# 			)

# 	# update the dataset with the id
# 	dataset = Dataset(**response.data[0])

# 	# do some monitoring
# 	monitoring.uploads_counter.inc()
# 	monitoring.upload_time.observe(dataset.copy_time)
# 	monitoring.upload_size.observe(dataset.file_size)

# 	logger.info(
# 		f'Created new dataset <ID={dataset.id}> with file {dataset.file_alias}. ({format_size(dataset.file_size)}). Took {dataset.copy_time:.2f}s.',
# 		extra={'token': token, 'user_id': user.id, 'dataset_id': dataset.id},
# 	)

# 	return dataset


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
