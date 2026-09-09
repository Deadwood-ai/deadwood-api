from typing import Annotated, Optional, List

import time
from fastapi import UploadFile, Depends, HTTPException, Form, APIRouter
from fastapi.security import OAuth2PasswordBearer

from shared.models import StatusEnum, LicenseEnum, PlatformEnum, DatasetAccessEnum
from shared.db import verify_token
from shared.settings import settings
from shared.status import update_status
from shared.logging import LogCategory, LogContext, UnifiedLogger, SupabaseHandler
from shared.zip_utils import (
	ensure_supported_zip_compression,
	UnsupportedZipCompressionError,
	InvalidZipArchiveError,
)

from ..upload.upload import create_dataset_entry
from ..upload.chunk_session import locked_chunk_session
from ..upload.geotiff_processor import process_geotiff_upload
from ..upload.raw_images_processor import process_raw_images_upload
from ..utils.file_utils import UploadType, detect_upload_type


router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')

# Create logger instance
logger = UnifiedLogger(__name__)
# Add Supabase handler after initialization
logger.add_supabase_handler(SupabaseHandler())


@router.post('/datasets/chunk')
def upload_chunk(
	file: UploadFile,
	chunk_index: Annotated[int, Form(ge=0)],
	chunks_total: Annotated[int, Form(gt=0)],
	upload_id: Annotated[str, Form(min_length=1, max_length=128, pattern=r'^[A-Za-z0-9_-]+$')],
	token: Annotated[str, Depends(oauth2_scheme)],
	# Dataset required fields
	license: Annotated[LicenseEnum, Form()],
	platform: Annotated[PlatformEnum, Form()],
	authors: Annotated[List[str], Form()],  # List of authors
	# Dataset optional fields
	project_id: Annotated[Optional[str], Form()] = None,
	aquisition_year: Annotated[Optional[int], Form()] = None,
	aquisition_month: Annotated[Optional[int], Form()] = None,
	aquisition_day: Annotated[Optional[int], Form()] = None,
	additional_information: Annotated[Optional[str], Form()] = None,
	data_access: Annotated[DatasetAccessEnum, Form()] = DatasetAccessEnum.public,
	citation_doi: Annotated[Optional[str], Form()] = None,
	upload_type: Annotated[Optional[UploadType], Form()] = None,
):
	"""Handle chunked upload of files (GeoTIFF or ZIP) with auto-detection and simplified processing"""
	user = verify_token(token)
	if not user:
		logger.error('Invalid token provided for upload', LogContext(category=LogCategory.AUTH, token=token))
		raise HTTPException(status_code=401, detail='Invalid token')

	# Auto-detect upload type if not provided (backward compatibility)
	if upload_type is None:
		upload_type = detect_upload_type(file.filename)

	# Continue with upload processing for both types
	# Start upload timer
	t1 = time.time()

	if chunk_index >= chunks_total:
		raise HTTPException(status_code=422, detail='Chunk index must be less than chunks_total')

	upload_file_name = f'{upload_id}.tmp'
	# Write raw images directly to raw_images path, GeoTIFF to archive path
	if upload_type == UploadType.RAW_IMAGES_ZIP:
		upload_target_path = settings.raw_images_path / upload_file_name
	else:
		upload_target_path = settings.archive_path / upload_file_name

	# Log chunk upload start
	logger.info(
		f'Processing chunk {chunk_index + 1}/{chunks_total} for file {file.filename}',
		LogContext(
			category=LogCategory.UPLOAD,
			user_id=user.id,
			token=token,
			extra={'upload_id': upload_id, 'chunk_index': chunk_index, 'chunks_total': chunks_total},
		),
	)

	# Bind every request to the same immutable upload contract. Tokens may refresh.
	metadata = {
		'file_name': file.filename,
		'license': license,
		'platform': platform,
		'authors': authors,
		'project_id': project_id,
		'aquisition_year': aquisition_year,
		'aquisition_month': aquisition_month,
		'aquisition_day': aquisition_day,
		'additional_information': additional_information,
		'data_access': data_access,
		'citation_doi': citation_doi,
	}
	# This synchronous route runs in FastAPI's worker pool. The lock remains held
	# through all disk/DB work, including when the caller disconnects.
	with locked_chunk_session(
		settings.base_path / '.upload-sessions',
		upload_id,
		upload_target_path,
		str(user.id),
		{'chunks_total': chunks_total, 'upload_type': upload_type.value, **metadata},
	) as session:
		response = session.accept(chunk_index, file.file.read())
		if response is not None:
			return response
		session.begin_finalization()
		try:
			# Calculate upload runtime
			t2 = time.time()
			upload_runtime = t2 - t1

			# Validate ZIP compression methods before creating dataset entries.
			if upload_type == UploadType.RAW_IMAGES_ZIP:
				try:
					ensure_supported_zip_compression(upload_target_path)
				except UnsupportedZipCompressionError as e:
					upload_target_path.unlink(missing_ok=True)
					raise HTTPException(status_code=400, detail=str(e))
				except InvalidZipArchiveError as e:
					upload_target_path.unlink(missing_ok=True)
					raise HTTPException(status_code=400, detail=str(e))

			logger.info(
				f'Creating dataset entry for {file.filename}',
				LogContext(
					category=LogCategory.UPLOAD,
					user_id=user.id,
					token=token,
					extra={'upload_id': upload_id, 'file_name': file.filename},
				),
			)

			# Create dataset entry
			dataset = create_dataset_entry(user_id=user.id, token=token, **metadata)

			# Route to simplified processing based on upload type
			if upload_type == UploadType.GEOTIFF:
				# Call simplified GeoTIFF processing
				dataset = process_geotiff_upload(dataset, upload_target_path, token)
				file_name = f'{dataset.id}_ortho.tif'
				target_path = settings.archive_path / file_name
			elif upload_type == UploadType.RAW_IMAGES_ZIP:
				# Call simplified ZIP processing
				dataset = process_raw_images_upload(dataset, upload_target_path, token)
				file_name = f'{dataset.id}.zip'  # Actual ZIP filename
				target_path = settings.raw_images_path / file_name  # Actual file location, not directory

			file_size = target_path.stat().st_size

			logger.info(
				f'Upload completed successfully for dataset {dataset.id}',
				LogContext(
					category=LogCategory.UPLOAD,
					user_id=user.id,
					dataset_id=dataset.id,
					token=token,
					extra={
						'file_size': file_size,
						'upload_time': upload_runtime,
						'file_name': file_name,
					},
				),
			)

			response = dataset.model_dump(mode='json')
			session.complete(response)
			return response

		except (UnsupportedZipCompressionError, InvalidZipArchiveError) as e:
			logger.warning(
				f'ZIP validation failed: {str(e)}',
				LogContext(
					category=LogCategory.UPLOAD,
					user_id=user.id,
					dataset_id=dataset.id if 'dataset' in locals() else None,
					token=token,
					extra={'upload_id': upload_id, 'file_name': file.filename},
				),
			)
			if 'dataset' in locals():
				update_status(
					token=token,
					dataset_id=dataset.id,
					current_status=StatusEnum.uploading,
					has_error=True,
					error_message=str(e),
				)
			raise HTTPException(status_code=400, detail=str(e))
		except HTTPException:
			raise
		except Exception as e:
			logger.error(
				f'Error processing final chunk: {str(e)}',
				LogContext(
					category=LogCategory.UPLOAD,
					user_id=user.id,
					dataset_id=dataset.id if 'dataset' in locals() else None,
					token=token,
					extra={'upload_id': upload_id, 'file_name': file.filename, 'error': str(e)},
				),
			)
			# Update status to indicate error
			if 'dataset' in locals():
				update_status(
					token=token,
					dataset_id=dataset.id,
					current_status=StatusEnum.uploading,
					has_error=True,
					error_message=str(e),
				)
			raise HTTPException(status_code=500, detail=str(e))
