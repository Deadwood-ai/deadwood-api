import os
import paramiko
from pathlib import Path

from shared.logger import logger
from shared.settings import settings
from shared.models import StatusEnum
from shared.supabase import use_client


def pull_file_from_storage_server(remote_file_path: str, local_file_path: str, token: str):
	# Check if the file already exists locally
	if os.path.exists(local_file_path):
		logger.info(f'File already exists locally at: {local_file_path}')
		return

	with paramiko.SSHClient() as ssh:
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		pkey = paramiko.RSAKey.from_private_key_file(
			settings.SSH_PRIVATE_KEY_PATH, password=settings.SSH_PRIVATE_KEY_PASSPHRASE
		)
		logger.info(
			f'Connecting to storage server: {settings.STORAGE_SERVER_IP} as {settings.STORAGE_SERVER_USERNAME}',
			extra={'token': token},
		)

		ssh.connect(
			hostname=settings.STORAGE_SERVER_IP,
			username=settings.STORAGE_SERVER_USERNAME,
			pkey=pkey,
			port=22,  # Add this line to specify the default SSH port
		)

		with ssh.open_sftp() as sftp:
			logger.info(
				f'Pulling file from storage server: {remote_file_path} to {local_file_path}', extra={'token': token}
			)

			# Create the directory for local_file_path if it doesn't exist
			local_dir = Path(local_file_path).parent
			local_dir.mkdir(parents=True, exist_ok=True)
			sftp.get(remote_file_path, local_file_path)

		# Check if the file exists after pulling
		if os.path.exists(local_file_path):
			logger.info(f'File successfully saved at: {local_file_path}', extra={'token': token})
		else:
			logger.error(f'Error: File not found at {local_file_path} after pulling', extra={'token': token})


def push_file_to_storage_server(local_file_path: str, remote_file_path: str, token: str):
	if settings.DEV_MODE:
		logger.info(f'Skipping push to storage server in dev mode: {local_file_path} -> {remote_file_path}')
		return

	with paramiko.SSHClient() as ssh:
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		pkey = paramiko.RSAKey.from_private_key_file(
			settings.SSH_PRIVATE_KEY_PATH, password=settings.SSH_PRIVATE_KEY_PASSPHRASE
		)
		logger.info(
			f'Connecting to storage server: {settings.STORAGE_SERVER_IP} as {settings.STORAGE_SERVER_USERNAME}',
			extra={'token': token},
		)
		ssh.connect(
			hostname=settings.STORAGE_SERVER_IP,
			username=settings.STORAGE_SERVER_USERNAME,
			pkey=pkey,
			port=22,
		)

		with ssh.open_sftp() as sftp:
			# remote_dir = os.path.dirname(remote_file_path)
			temp_remote_path = f'{remote_file_path}.tmp'

			# try:
			# 	# Create directory if it doesn't exist (creates all parent directories)
			# 	sftp.mkdir(remote_dir, mode=0o755)
			# 	logger.info(f'Created directory {remote_dir}', extra={'token': token})
			# except IOError:
			# 	# Directory already exists, continue
			# 	pass

			try:
				# Upload to temporary location first
				logger.info(f'Uploading file to temporary location: {temp_remote_path}', extra={'token': token})
				sftp.put(local_file_path, temp_remote_path)

				# Atomic rename from temp to final location
				logger.info(f'Moving file to final location: {remote_file_path}', extra={'token': token})
				sftp.posix_rename(temp_remote_path, remote_file_path)
				logger.info(f'File successfully pushed to: {remote_file_path}', extra={'token': token})

			except Exception as e:
				# Clean up temp file if it exists
				try:
					sftp.remove(temp_remote_path)
					logger.info('Cleaned up temporary file after failure', extra={'token': token})
				except IOError:
					pass

				logger.error(f'Failed to push file to {remote_file_path}: {str(e)}', extra={'token': token})
				raise


def update_status(token: str, dataset_id: int, status: StatusEnum):
	"""Function to update the status field of a dataset about the cog calculation process.

	Args:
	    token (str): Supabase client session token
	    dataset_id (int): Unique id of the dataset
	    status (StatusEnum): The current status of the cog calculation process to set the dataset to
	"""
	try:
		with use_client(token) as client:
			client.table(settings.datasets_table).update(
				{
					'status': status.value,
				}
			).eq('id', dataset_id).execute()
	except Exception as e:
		logger.error(f'Error updating status: {e}', extra={'token': token})
