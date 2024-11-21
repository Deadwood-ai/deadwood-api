import os
import paramiko
from pathlib import Path

from ...shared.logger import logger
from ...shared.settings import settings
from ...shared.models import StatusEnum
from ...shared.supabase import use_client


def pull_file_from_storage_server(remote_file_path: str, local_file_path: str, token: str):
	# Check if the file already exists locally
	if os.path.exists(local_file_path):
		logger.info(f'File already exists locally at: {local_file_path}')
		return

	with paramiko.SSHClient() as ssh:
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		pkey = paramiko.RSAKey.from_private_key_file(
			settings.ssh_private_key_path, password=settings.ssh_private_key_passphrase
		)
		logger.info(
			f'Connecting to storage server: {settings.storage_server_ip} as {settings.storage_server_username}',
			extra={'token': token},
		)

		ssh.connect(
			hostname=settings.storage_server_ip,
			username=settings.storage_server_username,
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
	with paramiko.SSHClient() as ssh:
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		logger.info(
			f'Connecting to storage server: {settings.storage_server_ip} as {settings.storage_server_username}',
			extra={'token': token},
		)
		pkey = paramiko.RSAKey.from_private_key_file(
			settings.ssh_private_key_path, password=settings.ssh_private_key_passphrase
		)
		ssh.connect(
			hostname=settings.storage_server_ip,
			username=settings.storage_server_username,
			pkey=pkey,
			port=22,  # Add this line to specify the default SSH port
		)
		with ssh.open_sftp() as sftp:
			logger.info(
				f'Pushing file to storage server: {local_file_path} to {remote_file_path}', extra={'token': token}
			)

			# Extract the remote directory path
			remote_dir = os.path.dirname(remote_file_path)

			try:
				sftp.stat(remote_file_path)
				logger.warning(
					f'File {remote_file_path} already exists and will be overwritten', extra={'token': token}
				)
			except IOError:
				logger.info(f'No existing file found at {remote_file_path}', extra={'token': token})

			# Ensure the remote directory exists
			try:
				sftp.stat(remote_dir)
			except IOError:
				try:
					sftp.mkdir(remote_dir)
					logger.info(f'Created directory {remote_dir}', extra={'token': token})
				except IOError as e:
					logger.warning(f'Error creating directory {remote_dir}: {e}', extra={'token': token})

			# Push the file
			try:
				sftp.put(local_file_path, remote_file_path)
				logger.info(f'File successfully pushed to: {remote_file_path}', extra={'token': token})
			except IOError as e:
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
