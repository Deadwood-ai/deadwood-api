from typing import Callable
from enum import Enum
import tempfile
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import pandas as pd

from shared.__version__ import __version__
from shared.models import Dataset, Label, Metadata
from shared.settings import settings
from api.src.download.downloads import bundle_dataset, label_to_geopackage

# first approach to implement a rate limit
CONNECTED_IPS = {}

# create the router for download
download_app = FastAPI(
	title='Deadwood-AI Download API',
	description='This is the Deadwood-AI Download API. It is used to download single files and full Datasets. This is part of the Deadwood API.',
	version=__version__,
)

# add cors
download_app.add_middleware(
	CORSMiddleware,
	allow_origins=['*'],
	allow_credentials=False,
	allow_methods=['OPTIONS', 'GET'],
	allow_headers=['Content-Type', 'Accept', 'Accept-Encoding'],
)


# add the middleware for rate limiting
@download_app.middleware('http')
async def rate_limiting(request: Request, call_next: Callable[[Request], Response]):
	# get the ip
	ip = request.client.host

	# check if the IP is currently downloading
	if ip in CONNECTED_IPS:
		raise HTTPException(status_code=429, detail='Rate limit exceeded. You can only download one file at a time.')

	# set the ip
	CONNECTED_IPS[ip] = True

	# do the response
	try:
		response = await call_next(request)
		return response
	except Exception as e:
		raise HTTPException(status_code=500, detail=str(e))
	finally:
		# in any case delete the ip again
		del CONNECTED_IPS[ip]


# add the gzip middleware
download_app.add_middleware(GZipMiddleware)


# add the format model
class MetadataFormat(str, Enum):
	json = 'json'
	csv = 'csv'


@download_app.get('/')
def info():
	pass


# main download route
@download_app.get('/datasets/{dataset_id}')
@download_app.get('/datasets/{dataset_id}/dataset.zip')
async def download_dataset(dataset_id: str, background_tasks: BackgroundTasks):
	"""
	Download the full dataset with the given ID.
	This will create a ZIP file contianing the archived
	original GeoTiff, along with a JSON of the metadata and
	(for dev purpose now) a json schema of the metadata.
	If available, a GeoPackage with the labels will be added.

	"""
	# load the dataset
	dataset = Dataset.by_id(dataset_id)
	if dataset is None:
		raise HTTPException(status_code=404, detail=f'Dataset <ID={dataset_id}> not found.')

	# load the metadata
	metadata = Metadata.by_id(dataset_id)
	if metadata is None:
		raise HTTPException(status_code=404, detail=f'Dataset <ID={dataset_id}> has no associated Metadata entry.')



	# load the label
	# TODO: this loads immer nur das erste Label!!!
	label = Label.by_id(dataset_id)

	# build the file name
	archive_file_name = (settings.archive_path / dataset.file_name).resolve()

	# build a temporary zip location
	# TODO: we can use a caching here
	target = tempfile.NamedTemporaryFile(suffix='.zip', delete_on_close=False)
	try:
		bundle_dataset(target.name, archive_file_name, metadata=metadata, label=label)
	except Exception as e:
		msg = f'Failed to bundle dataset <ID={dataset_id}>: {str(e)}'
		raise HTTPException(status_code=500, detail=msg)
	finally:
		# remove the temporary file as a background_task
		background_tasks.add_task(lambda: Path(target.name).unlink())

	# now stream the file to the user
	return FileResponse(target.name, media_type='application/zip', filename=f'{Path(metadata.name).stem}.zip')


@download_app.get('/datasets/{dataset_id}/ortho.tif')
async def download_geotiff(dataset_id: str):
	"""
	Download the original GeoTiff of the dataset with the given ID.
	"""
	# load the dataset
	dataset = Dataset.by_id(dataset_id)

	if dataset is None:
		raise HTTPException(status_code=404, detail=f'Dataset <ID={dataset_id}> not found.')

	# here we can add the monitoring
	monitoring.download_ortho.inc()

	# build the file name
	path = settings.archive_path / dataset.file_name

	return FileResponse(path, media_type='image/tiff', filename=dataset.file_name)


@download_app.get('/datasets/{dataset_id}/metadata.{file_format}')
async def get_metadata(dataset_id: str, file_format: MetadataFormat, background_tasks: BackgroundTasks):
	"""
	Download the metadata of the dataset with the given ID.
	"""
	# load the metadata
	metadata = Metadata.by_id(dataset_id)
	if metadata is None:
		raise HTTPException(status_code=404, detail=f'Dataset <ID={dataset_id}> has no Metadata entry.')

	# switch the format
	if file_format == MetadataFormat.json:
		return metadata.model_dump_json()
	elif file_format == MetadataFormat.csv:
		# build a DataFrame
		df = pd.DataFrame.from_records([metadata.model_dump()])

		# create a temporary file
		target = tempfile.NamedTemporaryFile(suffix='.csv', delete_on_close=False)
		df.to_csv(target.name, index=False)

		# add a background task to remove the file after download
		background_tasks.add_task(lambda: Path(target.name).unlink())

		return FileResponse(target.name, media_type='text/csv', filename='metadata.csv')


@download_app.get('/datasets/{dataset_id}/labels.gpkg')
async def get_labels(dataset_id: str, background_tasks: BackgroundTasks):
	"""
	Download the labels of the dataset with the given ID.
	"""
	# load the labels
	label = Label.by_id(dataset_id=dataset_id)
	if label is None:
		raise HTTPException(status_code=404, detail=f'Dataset <ID={dataset_id}> has no labels.')

	# create a temporary file
	target = tempfile.NamedTemporaryFile(suffix='.gpkg', delete_on_close=False)

	# remove the file after download
	background_tasks.add_task(lambda: Path(target.name).unlink())

	# write the labels
	label_to_geopackage(target.name, label)

	# return the file
	return FileResponse(target.name, media_type='application/geopackage+sqlite', filename='labels.gpkg')
