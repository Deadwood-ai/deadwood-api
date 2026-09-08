import tempfile
import os
import uuid
import json
import time
import math
from dataclasses import dataclass
from pathlib import Path
import numpy as np
import rasterio
import rasterio.warp
from rasterio.windows import Window
import docker
import requests
import tarfile
import io

from shared.logger import logger
from shared.settings import settings
from shared.models import LabelPayloadData, LabelSourceEnum, LabelTypeEnum, LabelDataEnum
from shared.labels import create_label_with_geometries, delete_model_prediction_labels
from shared.logging import LogContext, LogCategory
from shared.db import login, verify_token
from ..utils.segmentation import (
	mask_to_polygons_scanline,
	reproject_polygons,
	filter_polygons_by_area,
	get_utm_string_from_latlon,
)
from processor.src.utils.shared_volume import cleanup_volume_and_references
from processor.src.utils.debug_artifacts import (
	retain_failed_artifacts_enabled_for_dataset,
	dt_resource_labels,
	build_container_forensics,
	write_debug_bundle,
)
from ..exceptions import ProcessingError, AuthenticationError

# Load configuration
CONFIG_PATH = str(Path(__file__).parent / 'treecover_inference_config.json')
with open(CONFIG_PATH, 'r') as f:
	config = json.load(f)

# TCD configuration
TCD_THRESHOLD = config['tree_cover_threshold']
TCD_MODEL = 'restor/tcd-segformer-mit-b5'
TCD_TARGET_RESOLUTION = config['tree_cover_inference_resolution']  # 10cm resolution (forced for all inputs)
TCD_TARGET_CRS = 'EPSG:3395'  # World Mercator - what the TCD model was trained on
TCD_OUTPUT_CRS = 'EPSG:4326'  # WGS84 for database storage
TCD_CONTAINER_IMAGE = settings.TCD_CONTAINER_IMAGE  # Our local TCD container
MODULE_NAME = 'treecover_segmentation_oam_tcd'
CHECKPOINT_NAME = TCD_MODEL


@dataclass(frozen=True)
class _TCDTimeoutPolicy:
	timeout_seconds: int
	base_timeout_seconds: int
	max_timeout_seconds: int
	base_pixels: int
	input_pixels: int | None
	capped: bool


class _TCDContainerTimeout(Exception):
	"""Raised when the TCD Docker wait call reaches its allowed wall time."""


def _is_docker_wait_timeout(exc: BaseException) -> bool:
	"""Return true for Docker SDK wait timeouts, including urllib3-wrapped forms."""
	if isinstance(exc, requests.exceptions.ReadTimeout):
		return True

	seen: set[int] = set()
	stack: list[BaseException | object] = [exc]
	while stack:
		current = stack.pop()
		if id(current) in seen:
			continue
		seen.add(id(current))

		class_name = current.__class__.__name__
		if class_name == 'ReadTimeoutError':
			return True

		if isinstance(current, BaseException):
			if current.__cause__ is not None:
				stack.append(current.__cause__)
			if current.__context__ is not None:
				stack.append(current.__context__)
			stack.extend(arg for arg in current.args if isinstance(arg, BaseException))

	return isinstance(exc, requests.exceptions.ConnectionError) and 'Read timed out' in str(exc)


def _compute_tcd_timeout_policy(input_width: int | None = None, input_height: int | None = None) -> _TCDTimeoutPolicy:
	"""
	Choose a TCD wall-time limit from configured base timeout and input size.

	TCD runtime scales with the number of pixels passed to the model. The configured
	timeout remains the floor for ordinary inputs; very large orthos get more time
	up to a separate cap so a single stuck container cannot block the processor
	indefinitely.
	"""
	base_timeout_seconds = max(1, settings.TCD_CONTAINER_TIMEOUT_SECONDS)
	max_timeout_seconds = max(base_timeout_seconds, settings.TCD_CONTAINER_TIMEOUT_MAX_SECONDS)
	base_pixels = max(1, settings.TCD_CONTAINER_TIMEOUT_BASE_PIXELS)
	input_pixels = None
	timeout_seconds = base_timeout_seconds

	if input_width is not None and input_height is not None and input_width > 0 and input_height > 0:
		input_pixels = input_width * input_height
		if input_pixels > base_pixels:
			timeout_seconds = math.ceil(base_timeout_seconds * input_pixels / base_pixels)

	capped = timeout_seconds > max_timeout_seconds
	timeout_seconds = min(timeout_seconds, max_timeout_seconds)
	return _TCDTimeoutPolicy(
		timeout_seconds=timeout_seconds,
		base_timeout_seconds=base_timeout_seconds,
		max_timeout_seconds=max_timeout_seconds,
		base_pixels=base_pixels,
		input_pixels=input_pixels,
		capped=capped,
	)


class _GeneratorStream(io.RawIOBase):
	"""
	Wraps a generator to provide a file-like interface for tarfile.

	Docker's get_archive() returns a generator, but tarfile.open() expects
	a file-like object with a .read() method. This wrapper bridges that gap
	by implementing the io.RawIOBase interface and streaming chunks from
	the generator without loading the entire archive into memory.
	"""

	def __init__(self, generator):
		self.generator = generator
		self.leftover = b''

	def readable(self):
		return True

	def readinto(self, b):
		try:
			length = len(b)
			chunk = self.leftover or next(self.generator)
			output, self.leftover = chunk[:length], chunk[length:]
			b[: len(output)] = output
			return len(output)
		except StopIteration:
			return 0  # Indicate EOF


MINIMUM_POLYGON_AREA = config['minimum_polygon_area']


def _reproject_orthomosaic_for_tcd(input_tif: str, output_path: str) -> str:
	"""
	Reproject orthomosaic to EPSG:3395 (World Mercator) with forced 10cm resolution for TCD.

	The TCD model was trained on EPSG:3395 at 10cm resolution. This function ensures
	all inputs are standardized to match the training conditions, regardless of the
	original CRS or resolution of the input orthomosaic.

	Args:
		input_tif (str): Path to input orthomosaic
		output_path (str): Path for reprojected output file

	Returns:
		str: Path to reprojected file
	"""
	with rasterio.open(input_tif) as src:
		# Calculate transform for EPSG:3395 at forced 10cm resolution
		target_transform, target_width, target_height = rasterio.warp.calculate_default_transform(
			src.crs,
			TCD_TARGET_CRS,
			src.width,
			src.height,
			*src.bounds,
			resolution=TCD_TARGET_RESOLUTION,  # Force 10cm regardless of input resolution
		)

		# Create output profile
		profile = src.profile.copy()
		profile.update(
			{
				'crs': TCD_TARGET_CRS,
				'transform': target_transform,
				'width': target_width,
				'height': target_height,
				'nodata': 0,
				'BIGTIFF': 'YES',
			}
		)

		# Reproject the image to EPSG:3395
		with rasterio.open(output_path, 'w', **profile) as dst:
			for i in range(1, src.count + 1):
				rasterio.warp.reproject(
					source=rasterio.band(src, i),
					destination=rasterio.band(dst, i),
					src_transform=src.transform,
					src_crs=src.crs,
					dst_transform=target_transform,
					dst_crs=TCD_TARGET_CRS,
					resampling=rasterio.warp.Resampling.bilinear,
				)

	return output_path


def _copy_files_to_tcd_volume(ortho_path: str, volume_name: str, dataset_id: int, token: str) -> tuple[str, str]:
	"""
	Copy reprojected orthomosaic and pipeline script to TCD shared volume.

	Args:
		ortho_path (str): Path to reprojected orthomosaic file
		volume_name (str): Docker volume name
		dataset_id (int): Dataset ID for directory structure
		token (str): Authentication token for logging

	Returns:
		tuple[str, str]: Container paths to (orthomosaic, confidence_map_output)
	"""
	client = docker.from_env()
	project_name = f'dataset_{dataset_id}'
	container_ortho_path = f'/tcd_data/{project_name}/input/orthomosaic.tif'
	container_confidence_path = f'/tcd_data/{project_name}/output/confidence_map.tif'

	logger.info(
		'Copying files to TCD shared volume',
		LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
	)

	# Create temporary container with named volume mounted
	temp_container = None
	try:
		container_name = f'dt-tcd-transfer-d{dataset_id}-{int(time.time())}-{uuid.uuid4().hex[:6]}'
		temp_container = client.containers.create(
			image='alpine',
			volumes={volume_name: {'bind': '/tcd_data', 'mode': 'rw'}},
			command=['tail', '-f', '/dev/null'],  # Keep alive for file operations (no timeout)
			name=container_name,
			user='root',
			auto_remove=False,
			labels={
				'dt': 'tcd',
				'dt_role': 'temp_transfer',
				'dt_dataset_id': str(dataset_id),
				'dt_volume': volume_name,
			},
		)
		temp_container.start()

		# Create TCD directory structure
		exec_result = temp_container.exec_run(
			f'mkdir -p /tcd_data/{project_name}/input /tcd_data/{project_name}/output'
		)
		if exec_result.exit_code != 0:
			raise Exception(f'Failed to create TCD directory structure: {exec_result.output.decode()}')

		# Copy orthomosaic file (stream to avoid loading entire file in memory)
		ortho_file = Path(ortho_path)

		# Copy pipeline script
		pipeline_script_path = Path(__file__).parent / 'predict_pipeline.py'
		with open(pipeline_script_path, 'rb') as f:
			script_data = f.read()

		# Create tar archive with both files
		tar_buffer = io.BytesIO()
		with tarfile.open(mode='w', fileobj=tar_buffer) as tar:
			# Add orthomosaic (streamed)
			ortho_info = tarfile.TarInfo(name='orthomosaic.tif')
			ortho_info.size = ortho_file.stat().st_size
			with open(ortho_file, 'rb') as of:
				tar.addfile(ortho_info, of)

			# Add pipeline script
			script_info = tarfile.TarInfo(name='predict_pipeline.py')
			script_info.size = len(script_data)
			tar.addfile(script_info, io.BytesIO(script_data))

		tar_buffer.seek(0)
		temp_container.put_archive(f'/tcd_data/{project_name}/input/', tar_buffer.getvalue())

		# Copy script to root of volume for entrypoint access
		script_tar_buffer = io.BytesIO()
		with tarfile.open(mode='w', fileobj=script_tar_buffer) as tar:
			script_info = tarfile.TarInfo(name='predict_pipeline.py')
			script_info.size = len(script_data)
			tar.addfile(script_info, io.BytesIO(script_data))

		script_tar_buffer.seek(0)
		temp_container.put_archive('/tcd_data/', script_tar_buffer.getvalue())

		logger.info(
			'Successfully copied files to TCD volume',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		return container_ortho_path, container_confidence_path

	except Exception as e:
		logger.error(
			f'Failed to copy files to TCD volume: {str(e)}',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)
		raise
	finally:
		if temp_container:
			try:
				temp_container.remove(force=True)
			except Exception as cleanup_error:
				logger.warning(
					f'Failed to cleanup TCD copy container: {cleanup_error}',
					LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
				)


def _run_tcd_pipeline_container(
	volume_name: str,
	dataset_id: int,
	token: str,
	input_width: int | None = None,
	input_height: int | None = None,
) -> str:
	"""
	Execute TCD container using Pipeline class via Python script for complete confidence map output.

	Args:
		volume_name (str): Docker volume name
		dataset_id (int): Dataset ID
		token (str): Authentication token for logging

	Returns:
		str: Container path to confidence map output file
	"""
	client = docker.from_env()
	project_name = f'dataset_{dataset_id}'
	input_path = f'/tcd_data/{project_name}/input/orthomosaic.tif'
	output_path = f'/tcd_data/{project_name}/output/confidence_map.tif'
	retain_on_failure = retain_failed_artifacts_enabled_for_dataset(dataset_id)
	resource_labels = dt_resource_labels(dataset_id=dataset_id, stage='tcd', keep_eligible=retain_on_failure)

	logger.info(
		f'Starting TCD Pipeline container execution for dataset {dataset_id}',
		LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
	)

	timeout_policy = _compute_tcd_timeout_policy(input_width=input_width, input_height=input_height)
	tcd_timeout_seconds = timeout_policy.timeout_seconds
	logger.info(
		'TCD container timeout policy: '
		f'timeout_seconds={timeout_policy.timeout_seconds}, '
		f'base_timeout_seconds={timeout_policy.base_timeout_seconds}, '
		f'max_timeout_seconds={timeout_policy.max_timeout_seconds}, '
		f'base_pixels={timeout_policy.base_pixels}, '
		f'input_pixels={timeout_policy.input_pixels}, '
		f'capped={timeout_policy.capped}',
		LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
	)

	try:
		# Preflight: ensure image exists
		try:
			client.images.get(TCD_CONTAINER_IMAGE)
		except Exception as e:
			raise Exception(f'TCD container image {TCD_CONTAINER_IMAGE} not found. Build or pull it. Error: {e}')

		def _run(use_gpu=False):
			# Expose the GPU the same way the processor container itself does
			# (docker-compose.processor.yaml): the legacy nvidia runtime +
			# NVIDIA_VISIBLE_DEVICES, which makes the nvidia runtime inject the GPU
			# device nodes. We deliberately do NOT use device_requests / `--gpus`,
			# because that CDI path is broken on the production host (`failed to
			# fulfil mount request: open /usr/bin/nvidia-cuda-mps-control`). The
			# legacy runtime+env path is the one proven to work on prod for Deadwood.
			return client.containers.run(
				image=TCD_CONTAINER_IMAGE,
				command=['python', '/tcd_data/predict_pipeline.py', input_path, output_path],
				entrypoint='',
				volumes={volume_name: {'bind': '/tcd_data', 'mode': 'rw'}},
				remove=False,
				detach=True,
				user='root',
				runtime='nvidia' if use_gpu else None,
				environment={
					'NVIDIA_VISIBLE_DEVICES': 'all',
					'NVIDIA_DRIVER_CAPABILITIES': 'compute,utility',
				}
				if use_gpu
				else {},
				labels={
					**resource_labels,
					'dt_role': 'tcd_pipeline',
					'dt_volume': volume_name,
				},
				name=f'dt-tcd-pipeline-d{dataset_id}-{int(time.time())}-{uuid.uuid4().hex[:6]}',
			)

		def _run_and_wait(use_gpu=False):
			"""Run container in detached mode with a timeout, then collect output."""
			container = _run(use_gpu=use_gpu)
			success = False
			try:
				try:
					result = container.wait(timeout=tcd_timeout_seconds)
				except requests.exceptions.RequestException as e:
					if not _is_docker_wait_timeout(e):
						raise

					logger.error(
						f'TCD container timed out after {tcd_timeout_seconds}s for dataset {dataset_id}',
						LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
					)
					try:
						container.kill()
					except Exception:
						pass

					try:
						forensics = build_container_forensics(
							container,
							dataset_id=dataset_id,
							stage='treecover',
							command=['python', '/tcd_data/predict_pipeline.py', input_path, output_path],
							volume_name=volume_name,
						)
						write_debug_bundle(forensics=forensics, token=token, dataset_id=dataset_id, stage='treecover')
					except Exception:
						pass

					timeout_hours = max(1, round(tcd_timeout_seconds / 3600))
					raise _TCDContainerTimeout(f'TCD container timed out after {timeout_hours} hours') from e

				container_output = container.logs()
				if result['StatusCode'] != 0:
					logs = container.logs(tail=200).decode('utf-8', errors='replace')

					# Persist debug bundle before cleanup so failures remain debuggable.
					forensics = build_container_forensics(
						container,
						dataset_id=dataset_id,
						stage='treecover',
						command=['python', '/tcd_data/predict_pipeline.py', input_path, output_path],
						volume_name=volume_name,
					)
					write_debug_bundle(forensics=forensics, token=token, dataset_id=dataset_id, stage='treecover')

					raise Exception(f'TCD exited with code {result["StatusCode"]}: {logs}')
				success = True
				return container_output
			finally:
				if success or not retain_on_failure:
					try:
						container.remove(force=True)
					except Exception:
						pass
				else:
					logger.warning(
						f'Retaining failed TCD container for debugging (DT_RETAIN_FAILED_ARTIFACTS enabled): {getattr(container, "name", None)}',
						LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
					)

		try:
			container_output = _run_and_wait(use_gpu=True)
		except _TCDContainerTimeout:
			raise
		except Exception as gpu_err:
			logger.warning(
				f'TCD container GPU execution failed, retrying once on CPU: {gpu_err}',
				LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
			)
			container_output = _run_and_wait(use_gpu=False)

		logger.info(
			'TCD Pipeline container execution completed successfully',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		# Log container output for debugging
		if container_output:
			logger.info(
				f'TCD Pipeline output: {container_output.decode("utf-8").strip()}',
				LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
			)

		return output_path

	except Exception as e:
		logger.error(
			f'TCD Pipeline container execution failed: {str(e)}',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)
		raise


def _copy_confidence_map_from_volume(volume_name: str, local_output_dir: Path, dataset_id: int, token: str) -> str:
	"""
	Copy TCD Pipeline confidence map from shared volume to local directory.

	Args:
		volume_name (str): Docker volume name
		local_output_dir (Path): Local directory to copy results to
		dataset_id (int): Dataset ID
		token (str): Authentication token for logging

	Returns:
		str: Path to extracted confidence_map.tif file
	"""
	client = docker.from_env()
	project_name = f'dataset_{dataset_id}'
	confidence_map_path = local_output_dir / 'confidence_map.tif'
	container_confidence_path = f'/tcd_data/{project_name}/output/confidence_map.tif'

	logger.info(
		f'Copying TCD Pipeline confidence map from shared volume to {local_output_dir}',
		LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
	)

	# Ensure output directory exists
	local_output_dir.mkdir(parents=True, exist_ok=True)

	# Create temporary container with shared volume mounted
	temp_container = None
	try:
		container_name = f'dt-tcd-extract-d{dataset_id}-{int(time.time())}-{uuid.uuid4().hex[:6]}'
		temp_container = client.containers.create(
			image='alpine',
			volumes={volume_name: {'bind': '/tcd_data', 'mode': 'ro'}},
			command=['tail', '-f', '/dev/null'],  # Keep alive for file operations (no timeout)
			name=container_name,
			user='root',
			auto_remove=True,
			labels={
				'dt': 'tcd',
				'dt_role': 'temp_extract',
				'dt_dataset_id': str(dataset_id),
				'dt_volume': volume_name,
			},
		)
		temp_container.start()

		# List all files in the output directory for debugging
		exec_result = temp_container.exec_run(f'ls -la /tcd_data/{project_name}/output/')
		logger.info(
			f'TCD output directory contents: {exec_result.output.decode()}',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		# Check if confidence_map.tif exists
		exec_result = temp_container.exec_run(f'test -f {container_confidence_path}')
		if exec_result.exit_code != 0:
			raise Exception(f'Confidence map not found at: {container_confidence_path}')

		logger.info(
			f'Found confidence map at: {container_confidence_path}',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		# Get confidence map file from container
		archive_stream, _ = temp_container.get_archive(container_confidence_path)

		# Extract confidence map to local directory
		# FIXED: Wrap generator in file-like object for tarfile streaming
		# Docker's get_archive() returns a generator, but tarfile expects a file-like object
		# This prevents memory exhaustion on large confidence maps while properly handling the generator
		start_time = time.time()
		file_count = 0
		total_bytes = 0

		wrapped_stream = io.BufferedReader(_GeneratorStream(archive_stream))
		with tarfile.open(mode='r|*', fileobj=wrapped_stream) as tar:
			for member in tar:
				# Explicit filter avoids upcoming Python 3.14 default-behavior warnings and
				# protects against path traversal / unsafe metadata.
				tar.extract(member, local_output_dir, filter='data')
				file_count += 1
				total_bytes += member.size

		# Log extraction stats
		elapsed = time.time() - start_time
		logger.info(
			f'Successfully copied confidence map to {confidence_map_path}: {file_count} files, {total_bytes / 1024 / 1024:.1f} MB in {elapsed:.1f}s',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		return str(confidence_map_path)

	except Exception as e:
		logger.error(
			f'Failed to copy confidence map from volume: {str(e)}',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)
		raise
	finally:
		if temp_container:
			try:
				temp_container.remove(force=True)
			except Exception as cleanup_error:
				logger.warning(
					f'Failed to cleanup TCD extraction container: {cleanup_error}',
					LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
				)


def _cleanup_tcd_volume(volume_name: str, dataset_id: int, token: str):
	"""
	Clean up TCD shared volume after processing.

	Args:
		volume_name (str): Docker volume name to remove
		dataset_id (int): Dataset ID for logging
		token (str): Authentication token for logging
	"""
	client = docker.from_env()

	try:
		client.volumes.get(volume_name).remove()
		logger.info(
			f'TCD shared volume {volume_name} removed successfully',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)
	except Exception as e:
		logger.warning(
			f'Failed to remove TCD shared volume {volume_name}: {str(e)}',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)


def predict_treecover(dataset_id: int, file_path: Path, user_id: str, token: str):
	"""
	Tree cover prediction using TCD Pipeline class in container for complete confidence map.

	This function implements the Pipeline-based approach:
	1. Preprocess: Reproject orthomosaic to EPSG:3395 (TCD requires metric CRS, not degrees)
	2. Container: Execute TCD Pipeline class (no additional reprojection in container)
	3. Postprocess: Load confidence map, apply nodata mask, threshold, filter polygons
	4. Storage: Convert to EPSG:4326 and save to v2_forest_cover_geometries via labels system

	Args:
		dataset_id (int): Dataset ID
		file_path (Path): Path to original orthomosaic file
		user_id (str): User ID for label creation
		token (str): Authentication token
	"""
	volume_name = None
	temp_dir = None
	had_error = False
	retain_on_failure = retain_failed_artifacts_enabled_for_dataset(dataset_id)
	resource_labels = dt_resource_labels(dataset_id=dataset_id, stage='tcd', keep_eligible=retain_on_failure)

	try:
		# Create temporary directory for processing
		temp_dir = tempfile.mkdtemp(prefix=f'treecover_{dataset_id}_')
		temp_dir_path = Path(temp_dir)

		# Step 1: Preprocess - reproject orthomosaic to EPSG:3395 for TCD
		# This is necessary because TCD expects metric CRS, not geographic (degrees)
		reprojected_temp_path = temp_dir_path / 'reprojected_orthomosaic.tif'
		logger.info(
			f'Reprojecting orthomosaic to EPSG:3395 for TCD: {file_path} -> {reprojected_temp_path}',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)
		reprojected_path = Path(_reproject_orthomosaic_for_tcd(str(file_path), str(reprojected_temp_path)))
		with rasterio.open(str(reprojected_path)) as reprojected_src:
			reprojected_width = reprojected_src.width
			reprojected_height = reprojected_src.height

		# Step 2: Container Setup - Create shared volume and copy reprojected ortho
		volume_name = f'tcd_volume_{dataset_id}_{uuid.uuid4().hex[:8]}'
		client = docker.from_env()
		client.volumes.create(name=volume_name, labels=resource_labels)

		logger.info(
			f'Created TCD shared volume {volume_name}',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		# Copy reprojected orthomosaic and pipeline script to shared volume
		container_ortho_path, container_confidence_path = _copy_files_to_tcd_volume(
			str(reprojected_path), volume_name, dataset_id, token
		)

		# Step 3: Container Execution - Run TCD Pipeline container for complete confidence map
		logger.info(
			'Running TCD Pipeline container for complete confidence map generation',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		_run_tcd_pipeline_container(
			volume_name,
			dataset_id,
			token,
			input_width=reprojected_width,
			input_height=reprojected_height,
		)

		# Refresh token before extraction - TCD containers can run for hours and token may have expired
		token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)
		logger.info(
			'Token refreshed before result extraction',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		# Step 4: Result Extraction - Copy confidence map from volume
		tcd_output_dir = temp_dir_path / 'tcd_output'
		confidence_map_path = _copy_confidence_map_from_volume(volume_name, tcd_output_dir, dataset_id, token)

		# Step 5: Postprocessing - threshold the confidence map and apply the nodata
		# mask, writing the binary result to a temporary GeoTIFF block-by-block.
		#
		# The TCD confidence map is gigapixel-scale (e.g. ~40000x22000 px for a
		# typical drone ortho). The previous approach loaded the whole confidence
		# array, the full thresholded array AND the full dataset mask at once — plus
		# the float64 temporary that ``mask / 255`` created — peaking at many GB and
		# OOM-killing the worker mid-step (SIGKILLed, so nothing was ever logged).
		# We now stream block-by-block to a binary mask GeoTIFF and polygonize it
		# with ``mask_to_polygons_scanline`` (the same memory-safe path the deadwood,
		# combined and AOI models already use), so no full-resolution array is ever
		# held in memory — peak usage stays proportional to a single window.
		logger.info(
			'Loading confidence map and applying thresholding',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)
		logger.info(
			'Applying nodata mask processing to filter invalid areas',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		treecover_mask_path = temp_dir_path / 'treecover_mask.tif'
		with rasterio.open(confidence_map_path) as confidence_src:
			height = confidence_src.height
			width = confidence_src.width

			# For internally tiled GeoTIFFs, iterate the native tiles. For non-tiled
			# (striped) files block_windows yields one window per strip — often a
			# single row — which is correct but means tens of thousands of tiny
			# reads on a gigapixel raster; coalesce those into 2048-row strips
			# instead. Either way peak memory stays proportional to one window.
			if confidence_src.is_tiled:
				windows = [window for _, window in confidence_src.block_windows(1)]
			else:
				strip_height = 2048
				windows = [
					Window(0, row, width, min(strip_height, height - row))
					for row in range(0, height, strip_height)
				]

			mask_profile = confidence_src.profile.copy()
			mask_profile.update(
				count=1, dtype='uint8', nodata=None,
				compress='LZW', tiled=True, blockxsize=512, blockysize=512,
			)

			# Pass 1: threshold each confidence block straight into the output
			# GeoTIFF and record which nodata mask values appear, without ever
			# materialising a full-resolution array.
			mask_values_seen: set[int] = set()
			with rasterio.open(treecover_mask_path, 'w', **mask_profile) as mask_dst:
				for window in windows:
					confidence_block = confidence_src.read(1, window=window)
					mask_dst.write((confidence_block > TCD_THRESHOLD).astype(np.uint8), 1, window=window)
					mask_values_seen.update(np.unique(confidence_src.dataset_mask(window=window)).tolist())

			# Only apply masking if the mask is strictly binary (values ⊆ {0, 255}).
			# The block-wise pass zeros pixels where mask == 0, which is only correct
			# for a true binary nodata mask; partial-alpha masks (e.g. {0, 128}) are
			# left untouched to avoid masking artifacts.
			unique_mask_values = sorted(mask_values_seen)
			if mask_values_seen and mask_values_seen.issubset({0, 255}):
				# Pass 2: zero out fully-transparent (mask == 0) pixels block-by-block,
				# reading and writing the output GeoTIFF in place.
				with rasterio.open(treecover_mask_path, 'r+') as mask_dst:
					for window in windows:
						mask_block = confidence_src.dataset_mask(window=window)
						out_block = mask_dst.read(1, window=window)
						out_block[mask_block == 0] = 0
						mask_dst.write(out_block, 1, window=window)
				logger.info(
					f'Applied standard nodata mask with values: {unique_mask_values}',
					LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
				)
			else:
				# Non-standard mask with values all over the place - skip masking
				logger.warning(
					f'Non-standard mask detected with values: {unique_mask_values} - skipping masking operation to avoid artifacts',
					LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
				)

		# Step 6: Polygon Conversion - polygonize the binary mask GeoTIFF scanline by
		# scanline so the full raster is never loaded into memory.
		logger.info(
			'Converting binary mask to polygons',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		with rasterio.open(str(treecover_mask_path)) as dataset:
			polygons = mask_to_polygons_scanline(dataset, 1)

		if len(polygons) == 0:
			logger.warning(
				'No tree cover polygons detected',
				LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
			)
			return

		# Choose metric CRS for area filtering, then reproject to WGS84
		logger.info(
			'Preparing polygons for area filtering and reprojection',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		with rasterio.open(str(confidence_map_path)) as dataset:
			source_crs = dataset.crs
			bounds = dataset.bounds
			center_x = (bounds.left + bounds.right) / 2.0
			center_y = (bounds.bottom + bounds.top) / 2.0

		# Select area CRS
		area_crs = None
		try:
			if source_crs and not source_crs.is_geographic:
				area_crs = source_crs.to_string()
			else:
				area_crs = get_utm_string_from_latlon(center_y, center_x)
		except Exception:
			area_crs = source_crs.to_string() if source_crs else 'EPSG:3857'

		if area_crs and source_crs and area_crs != source_crs.to_string():
			polygons = reproject_polygons(polygons, source_crs, area_crs)

		logger.info(
			f'Filtering {len(polygons)} polygons by minimum area of {MINIMUM_POLYGON_AREA}m²',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)
		polygons = filter_polygons_by_area(polygons, MINIMUM_POLYGON_AREA)
		if len(polygons) == 0:
			logger.warning(
				'No tree cover polygons detected after area filtering',
				LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
			)
			return

		# Reproject to WGS84 for storage
		if area_crs:
			polygons = reproject_polygons(polygons, area_crs, TCD_OUTPUT_CRS)
		else:
			polygons = reproject_polygons(polygons, source_crs, TCD_OUTPUT_CRS)

		# Step 6.5: Validate and fix geometries before saving (CRITICAL for frontend performance)
		logger.info(
			'Validating and fixing geometries before database storage',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)
		from processor.src.utils.geometry_validation import validate_and_fix_polygons

		polygons, validation_stats = validate_and_fix_polygons(
			polygons, min_area=0.0, dataset_id=dataset_id, label_type='treecover'
		)

		if len(polygons) == 0:
			logger.warning(
				'No valid tree cover polygons after geometry validation',
				LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
			)
			return

		# Step 7: Database Storage - Convert to GeoJSON and save via labels system
		logger.info(
			'Converting polygons to GeoJSON format for database storage',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		# Convert polygons to GeoJSON MultiPolygon format
		treecover_geojson = {
			'type': 'MultiPolygon',
			'coordinates': [
				[[[float(x), float(y)] for x, y in poly.exterior.coords]]
				+ [[[float(x), float(y)] for x, y in interior.coords] for interior in poly.interiors]
				for poly in polygons
			],
		}

		# Create label payload
		# Derive actual output resolution from the confidence map dataset to record in properties
		with rasterio.open(str(confidence_map_path)) as dataset:
			out_xres, out_yres = dataset.res
			actual_resolution_m = float(max(abs(out_xres), abs(out_yres)))

		payload = LabelPayloadData(
			dataset_id=dataset_id,
			label_source=LabelSourceEnum.model_prediction,
			label_type=LabelTypeEnum.semantic_segmentation,
			label_data=LabelDataEnum.forest_cover,
			label_quality=3,
			model_metadata={
				'module': MODULE_NAME,
				'checkpoint_name': CHECKPOINT_NAME,
			},
			geometry=treecover_geojson,
			properties={
				'model': TCD_MODEL,
				'threshold': TCD_THRESHOLD,
				'resolution_m': actual_resolution_m,
				'processing_crs': source_crs.to_string() if source_crs else None,
				'container_version': TCD_CONTAINER_IMAGE,
			},
		)

		# Refresh token before database operations to avoid expiry after long inference
		token = login(settings.PROCESSOR_USERNAME, settings.PROCESSOR_PASSWORD)
		user = verify_token(token)
		if not user:
			logger.error(
				'Token refresh failed during treecover database operations',
				LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
			)
			raise AuthenticationError('Token refresh failed', token=token, dataset_id=dataset_id)

		# Delete existing tree cover prediction labels
		deleted_count = delete_model_prediction_labels(
			dataset_id=dataset_id, label_data=LabelDataEnum.forest_cover, token=token
		)
		if deleted_count > 0:
			logger.info(
				f'Deleted {deleted_count} existing tree cover prediction labels',
				LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
			)

		# Create label with geometries
		logger.info(
			'Creating label with forest cover geometries',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

		label = create_label_with_geometries(payload, user_id, token)

		logger.info(
			f'Successfully created tree cover label {label.id} with {len(polygons)} geometries',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)

	except Exception as e:
		had_error = True
		logger.error(
			f'Error in predict_treecover: {str(e)}',
			LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
		)
		# Best-effort: persist a minimal debug bundle even if failure isn't a clean container exit.
		try:
			write_debug_bundle(
				forensics={
					'dataset_id': dataset_id,
					'stage': 'treecover',
					'error': str(e),
					'volume_name': volume_name,
					'temp_dir': temp_dir,
				},
				token=token,
				dataset_id=dataset_id,
				stage='treecover',
			)
		except Exception:
			pass
		raise ProcessingError(str(e), task_type='treecover_segmentation', dataset_id=dataset_id)

	finally:
		# Clean up resources
		if volume_name:
			if had_error and retain_on_failure:
				logger.warning(
					f'Retaining failed TCD volume for debugging (DT_RETAIN_FAILED_ARTIFACTS enabled): {volume_name}',
					LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
				)
			else:
				try:
					cleanup_volume_and_references(volume_name, token, dataset_id)
				except Exception:
					_cleanup_tcd_volume(volume_name, dataset_id, token)

		if temp_dir and os.path.exists(temp_dir):
			import shutil

			if had_error and retain_on_failure:
				logger.warning(
					f'Retaining treecover temp directory for debugging (DT_RETAIN_FAILED_ARTIFACTS enabled): {temp_dir}',
					LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
				)
			else:
				shutil.rmtree(temp_dir, ignore_errors=True)
				logger.info(
					f'Cleaned up temporary directory {temp_dir}',
					LogContext(category=LogCategory.TREECOVER, token=token, dataset_id=dataset_id),
				)
