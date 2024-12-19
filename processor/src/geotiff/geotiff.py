import subprocess
from shared.logger import logger


def convert_geotiff(input_path: str, output_path: str, token: str = None) -> bool:
	"""Convert GeoTIFF using gdalwarp with optimized parameters"""
	cmd = [
		'gdalwarp',
		'-co',
		'COMPRESS=DEFLATE',
		'-co',
		'PREDICTOR=2',
		'-co',
		'TILED=YES',
		'-co',
		'BLOCKSIZE=512',
		'-wo',
		'NUM_THREADS=ALL_CPUS',
		'-multi',
		'-co',
		'BIGTIFF=YES',
		input_path,
		output_path,
	]

	try:
		logger.info('Running gdalwarp conversion', extra={'token': token})
		result = subprocess.run(cmd, check=True, capture_output=True, text=True)
		logger.info(f'gdalwarp output:\n{result.stdout}', extra={'token': token})
		return True
	except subprocess.CalledProcessError as e:
		logger.error(f'Error during gdalwarp conversion: {e}', extra={'token': token})
		return False


def verify_geotiff(file_path: str, token: str = None) -> bool:
	"""Verify GeoTIFF file integrity"""
	try:
		cmd = ['gdalinfo', file_path]
		result = subprocess.run(cmd, check=True, capture_output=True, text=True)
		return 'ERROR' not in result.stdout and 'FAILURE' not in result.stdout
	except subprocess.CalledProcessError as e:
		logger.error(f'File verification failed: {e}', extra={'token': token})
		return False
