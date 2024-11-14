from pathlib import Path
import hashlib
import rasterio
from rasterio.env import Env
from concurrent.futures import ProcessPoolExecutor
import asyncio
import shutil
from fastapi import APIRouter, UploadFile, Depends, HTTPException, Form
from fastapi.security import OAuth2PasswordBearer

from shared.models import Dataset, StatusEnum
from shared.supabase import use_client, verify_token
from shared.settings import settings
from shared.logger import logger
from shared import monitoring
from .upload_service import UploadService
from .update_metadata_admin_level import update_metadata_admin_level


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

	# Initialize SHA256 hash object
	sha256_hash = hashlib.sha256()

	# Write directly to the target path
	with target_path.open('wb') as outfile:
		for i in range(int(total_chunks)):
			chunk_file = tmp_dir / f'chunk_{i}'
			with chunk_file.open('rb') as infile:
				while True:
					# Read in larger chunks to reduce I/O operations
					data = infile.read(1024 * 1024)  # 1MB buffer size
					if not data:
						break
					outfile.write(data)
					sha256_hash.update(data)
			chunk_file.unlink()  # Remove the chunk file after combining

	logger.info(f'Combined chunks for file {filename}', extra={'token': token})

	# Compute SHA256 checksum
	sha256 = sha256_hash.hexdigest()
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
