from typing import Annotated
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, Form
from fastapi.security import OAuth2PasswordBearer

from shared.supabase import verify_token, use_client
from shared.settings import settings
from shared.logger import logger
from shared.models import Dataset, Label, LabelPayloadData, UserLabelObject
from ..labels import verify_labels
from shared import monitoring

# create the router for the labels
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')


@router.post('/datasets/{dataset_id}/labels')
def create_new_labels(dataset_id: int, data: LabelPayloadData, token: Annotated[str, Depends(oauth2_scheme)]):
	""" """
	# count an invoke
	monitoring.label_invoked.inc()

	# first thing we do is verify the token
	user = verify_token(token)
	if not user:
		return HTTPException(status_code=401, detail='Invalid token')

	# load the the dataset info for this one
	try:
		with use_client(token) as client:
			# filter using the given dataset_id
			response = client.table(settings.datasets_table).select('*').eq('id', dataset_id).execute()

			# create the dataset
			dataset = Dataset(**response.data[0])
	except Exception as e:
		# log the error to the database
		msg = f'Error loading dataset {dataset_id}: {str(e)}'
		logger.error(msg, extra={'token': token, 'user_id': user.id, 'dataset_id': dataset_id})

		return HTTPException(status_code=500, detail=msg)

	# verify the data
	try:
		verify_labels(data.aoi, data.label)
	except Exception as e:
		# log the error to the database
		msg = f'Invalid label data: {str(e)}'
		logger.error(msg, extra={'token': token, 'user_id': user.id, 'dataset_id': dataset_id})

		return HTTPException(status_code=400, detail=msg)

	# fill the metadata
	meta = dict(
		dataset_id=dataset.id,
		user_id=user.id,
		aoi=data.aoi,
		label=data.label,
		label_source=data.label_source,
		label_quality=data.label_quality,
		label_type=data.label_type,
	)

	try:
		label = Label(**meta)
	except Exception as e:
		# log the error to the database
		msg = f'Error creating label object: {str(e)}'
		logger.error(msg, extra={'token': token, 'user_id': user.id, 'dataset_id': dataset.id})

		return HTTPException(status_code=400, detail=msg)

	# upload the dataset
	with use_client(token) as client:
		try:
			send_data = {k: v for k, v in label.model_dump().items() if k != 'id' and v is not None}
			response = client.table(settings.labels_table).insert(send_data).execute()
		except Exception as e:
			msg = f'An error occurred while trying to upload the label: {str(e)}'

			# log the error to the database
			logger.error(msg, extra={'token': token, 'dataset_id': dataset.id, 'user_id': user.id})
			return HTTPException(status_code=400, detail=msg)

	# re-build the label from the response
	label = Label(**response.data[0])

	# do some monitoring
	monitoring.label_counter.inc()
	logger.info(
		f'Created new label <ID={label.id}> for dataset {dataset_id}.',
		extra={'token': token, 'dataset_id': dataset_id, 'user_id': user.id},
	)

	return label


@router.post('/datasets/{dataset_id}/user-labels')
async def upload_user_labels(
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
	if not (settings.user_label_path / str(dataset_id)).exists():
		(settings.user_label_path / str(dataset_id)).mkdir(parents=True, exist_ok=True)
	# count number of files in the folder

	target_path = (
		settings.user_label_path
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
		file_alias=file_alias,  # alias is the original filename, as in the file_alias of the dataset
		file_path=str(target_path),  # path is the path to the label object on the server
		label_description=label_description,
		audited=False,
	)

	try:
		with use_client(token) as client:
			send_data = {k: v for k, v in label_object.model_dump().items() if v is not None}
			response = client.table(settings.label_objects_table).insert(send_data).execute()
			logger.info(
				f'Inserted label object into database: {response.data[0]}',
				extra={'token': token, 'dataset_id': dataset_id, 'user_id': user_id},
			)
	except Exception as e:
		logger.exception(f'Error inserting label object into database: {str(e)}', extra={'token': token})
		raise HTTPException(status_code=400, detail=f'Error inserting label object into database: {str(e)}')

	return label_object
