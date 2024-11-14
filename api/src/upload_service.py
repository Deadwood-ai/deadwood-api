from pathlib import Path
from typing import Optional
from datetime import datetime
import hashlib
import uuid
import rasterio
from rasterio.env import Env
from rasterio import warp


from shared.models import Dataset, StatusEnum
from shared.supabase import use_client
from shared.settings import settings
from shared.logger import logger
from .osm import get_admin_tags


class UploadService:
	def __init__(self, token: str):
		self.token = token

	def create_dataset_entry(
		self,
		file_path: Path,
		new_file_name: str,
		file_alias: str,
		user_id: str,
		copy_time: float,
		manual_upload: bool = False,
	) -> Dataset:
		"""Create initial dataset entry with file information"""
		if manual_upload:
			sha256 = self.compute_sha256(file_path)
			bounds = self.get_transformed_bounds(file_path)
			size = file_path.stat().st_size
		else:
			sha256 = None
			bounds = None
			size = None

		data = dict(
			file_name=new_file_name,
			file_alias=file_alias,
			file_size=size,
			copy_time=copy_time,
			sha256=sha256,
			bbox=bounds,
			status=StatusEnum.uploaded,
			user_id=user_id,
		)

		dataset = Dataset(**data)

		with use_client(self.token) as client:
			send_data = {k: v for k, v in dataset.model_dump().items() if k != 'id' and v is not None}
			response = client.table(settings.datasets_table).insert(send_data).execute()

		return Dataset(**response.data[0])

	def compute_sha256(self, file_path: Path) -> str:
		"""Compute SHA256 hash of file"""
		sha256_hash = hashlib.sha256()
		with file_path.open('rb') as f:
			for byte_block in iter(lambda: f.read(4096), b''):
				sha256_hash.update(byte_block)
		return sha256_hash.hexdigest()

	def get_transformed_bounds(self, file_path: Path):
		"""Get transformed bounds from GeoTIFF"""
		with Env(GTIFF_SRS_SOURCE='EPSG'):
			with rasterio.open(str(file_path), 'r') as src:
				bounds = src.bounds
				return warp.transform_bounds(src.crs, 'EPSG:4326', *bounds)
