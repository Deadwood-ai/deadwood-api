import os
import argparse
from pathlib import Path
import requests
import time

from src.supabase import login, verify_token
from src.utils.manual_upload import manual_upload
from src.settings import settings
from src.models import MetadataPayloadData
from src.utils.update_metadata_admin_level import update_metadata_admin_level


def main():
	parser = argparse.ArgumentParser(description='Manually upload a file to the storage server')
	parser.add_argument('--file_path', type=str, help='Path to the file to upload')
	parser.add_argument('--authors', type=str, help='Comma separated list of authors')
	parser.add_argument(
		'--data_access', type=str, default=os.getenv('DATA_ACCESS'), help='Data access level of the dataset'
	)
	parser.add_argument('--platform', type=str, help='Platform of the dataset')
	parser.add_argument('--aquisition_year', type=str, help='Aquisition year of the dataset')
	parser.add_argument('--aquisition_month', type=str, help='Aquisition month of the dataset')
	parser.add_argument('--aquisition_day', type=str, help='Aquisition day of the dataset')
	parser.add_argument('--doi', type=str, help='DOI of the dataset')
	parser.add_argument(
		'--additional_information',
		type=str,
		help='Additional metadata of the dataset',
	)
	parser.add_argument('--process', type=str, help='all, cog, thumbnail')

	args = parser.parse_args()
	print('arguments:', args)

	token = login(settings.processor_username, settings.processor_password)

	# Verify token and get user
	user = verify_token(token)
	if not user:
		raise ValueError('Invalid token')

	# Upload the file
	dataset = manual_upload(
		Path(args.file_path),
		token=token,
		user_id=user.id,  # Pass the verified user's ID
	)
	time.sleep(1)
	print('adding metadata for dataset:', dataset)

	metadata = MetadataPayloadData(
		name=dataset.file_alias,
		authors=args.authors,
		data_access=args.data_access,
		platform=args.platform,
		aquisition_year=args.aquisition_year,
		aquisition_month=args.aquisition_month,
		aquisition_day=args.aquisition_day,
		doi=args.doi,
		additional_information=args.additional_information,
	)
	print('metadata:', metadata)

	try:
		res = requests.put(
			f'{settings.api_endpoint}/datasets/{dataset.id}/metadata',
			json=metadata.model_dump(),
			headers={'Authorization': f'Bearer {token}'},
		)
		print('response:', res.json())
		res.raise_for_status()
	except requests.exceptions.RequestException as e:
		print(f'Error updating metadata: {e}')

	try:
		update_metadata_admin_level(dataset.id, token)
	except Exception as e:
		print(f'Error updating metadata admin level: {e}')

	try:
		res = requests.put(
			f'{settings.api_endpoint}/datasets/{dataset.id}/process',
			params={
				'task_type': args.process  # 'cog', 'thumbnail', or 'all'
			},
			headers={'Authorization': f'Bearer {token}'},
		)
		print('response:', res.json())
		res.raise_for_status()
	except Exception as e:
		print(f'Error updating process: {e}')


if __name__ == '__main__':
	main()
