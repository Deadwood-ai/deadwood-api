from pathlib import Path
import rasterio

from shared.supabase import use_client
from shared.settings import settings
from shared.models import GeoTiffInfo
from shared.logger import logger


def create_geotiff_info_entry(file_path: Path, dataset_id: int, token: str) -> GeoTiffInfo:
	"""
	Extract detailed metadata from a GeoTIFF file using rasterio and store it in the database.

	Args:
	    file_path (Path): Path to the GeoTIFF file
	    dataset_id (int): ID of the dataset in the database
	    token (str): Authentication token for Supabase

	Returns:
	    GeoTiffInfo: Object containing the extracted metadata
	"""
	try:
		with rasterio.open(str(file_path)) as src:
			# Basic file info
			info = GeoTiffInfo(
				dataset_id=dataset_id,
				driver=src.driver,
				size_width=src.width,
				size_height=src.height,
				file_size_gb=file_path.stat().st_size / (1024**3),
				# CRS info
				crs=src.crs.to_string(),
				crs_code=str(src.crs.to_epsg()),
				geodetic_datum=src.crs.to_dict().get('datum'),
				# Pixel and tiling info
				pixel_size_x=abs(src.transform.a),
				pixel_size_y=abs(src.transform.e),
				block_size_x=src.block_shapes[0][0],
				block_size_y=src.block_shapes[0][1],
				is_tiled=src.is_tiled,
				# Compression infos
				compression=src.compression.value if src.compression else None,
				interleave=src.interleaving.value if src.interleaving else None,
				is_bigtiff=src.is_tiled and file_path.stat().st_size > 4 * 1024 * 1024 * 1024,
				# Band information
				band_count=src.count,
				band_types=[src.dtypes[i] for i in range(src.count)],
				band_interpretations=[src.colorinterp[i].name for i in range(src.count)],
				band_nodata_values=[src.nodatavals[i] for i in range(src.count)],
				# Bounds information
				origin_x=src.transform.c,
				origin_y=src.transform.f,
				# Additional metadata
				extra_metadata=src.tags(),
			)
			# Store in database
			with use_client(token) as client:
				# Filter out None values
				send_data = {k: v for k, v in info.model_dump().items() if v is not None}

				# Insert into geotiff_info table
				response = client.table(settings.geotiff_info_table).upsert(send_data).execute()

				if response.data:
					logger.info(
						f'Stored GeoTIFF info for dataset {dataset_id}',
						extra={'token': token, 'dataset_id': dataset_id},
					)
					return info
				else:
					raise Exception('No data returned from database insert')

	except Exception as e:
		logger.error(
			f'Error extracting/storing GeoTIFF info for dataset {dataset_id}: {str(e)}',
			extra={'token': token, 'dataset_id': dataset_id},
		)
		raise
