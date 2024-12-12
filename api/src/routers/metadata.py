from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from shared.supabase import verify_token, use_client
from shared.settings import settings
from shared.models import Metadata, MetadataPayloadData
from shared.logger import logger
from shared import monitoring
from api.src.utils.admin_levels import update_metadata_admin_level

# create the router for the metadata
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')


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
	# monitoring.metadata_invoked.inc()

	# first thing we do is verify the token
	user = verify_token(token)
	if not user:
		raise HTTPException(status_code=401, detail='Invalid token')

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
		raise HTTPException(status_code=400, detail=msg)

	# update the given metadata if any with the payload
	try:
		metadata.update(**{k: v for k, v in payload.model_dump().items() if v is not None})
		metadata = Metadata(**metadata)
	except Exception as e:
		msg = f'An error occurred while trying to create the updated metadata: {str(e)}'

		logger.exception(msg, extra={'token': token, 'dataset_id': dataset_id, 'user_id': user.id})
		raise HTTPException(status_code=400, detail=msg)

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
		raise HTTPException(status_code=400, detail=err_msg)

	# no error occured, so return the upserted metadata
	logger.info(
		f'Upserted metadata for Dataset {dataset_id}. Upsert payload provided by user: {payload}',
		extra={'token': token, 'dataset_id': dataset_id, 'user_id': user.id},
	)

	# update the metadata
	logger.info(f'Updating admin level information for dataset {dataset_id}', extra={'token': token})
	try:
		update_metadata_admin_level(dataset_id, token)
	except Exception as e:
		logger.exception(
			f'Error updating admin level information for dataset {dataset_id}: {str(e)}', extra={'token': token}
		)
		raise HTTPException(
			status_code=400, detail=f'Error updating admin level information for dataset {dataset_id}: {str(e)}'
		)
	metadata = Metadata(**response.data[0])
	# monitoring.metadata_counter.inc()

	return metadata
