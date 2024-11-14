from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from pathlib import Path
import tempfile

# load an .env file if it exists
load_dotenv()


BASE = Path(__file__).parent.parent / 'data'


# load the settings from environment variables
class Settings(BaseSettings):
	# base directory for the storage app
	base_dir: str = str(BASE)

	# directly specify the locations for several files
	archive_dir: str = 'archive'
	cog_dir: str = 'cogs'
	thumbnails_dir: str = 'thumbnails'
	label_objects_dir: str = 'label_objects'

	# Temporary processing directory
	tmp_processing_path: str = str(Path(tempfile.mkdtemp(prefix='cog_processing_')))

	# supabase settings for supabase authentication
	supabase_url: Optional[str] = None
	supabase_key: Optional[str] = None

	# some basic settings for the UVICORN server
	uvicorn_host: str = '127.0.0.1'
	uvicorn_port: int = 8000
	uvicorn_root_path: str = ''
	uvicorn_proxy_headers: bool = True

	# storage server settings
	storage_server_ip: str = ''
	storage_server_username: str = ''
	storage_server_data_path: str = ''

	# api endpoint
	api_endpoint: str = 'https://data.deadtrees.earth/api/v1/'
	api_endpoint_datasets: str = api_endpoint + 'datasets/'

	# processor settings
	processor_username: str = 'processor@deadtrees.earth'
	processor_password: str = 'processor'
	ssh_private_key_path: str = '/app/ssh_key'
	ssh_private_key_passphrase: str = ''

	# tabe names
	datasets_table: str = 'v1_datasets'
	metadata_table: str = 'v1_metadata'
	cogs_table: str = 'v1_cogs'
	labels_table: str = 'v1_labels'
	thumbnail_table: str = 'v1_thumbnails'
	logs_table: str = 'logs'
	label_objects_table: str = 'v1_label_objects'

	# queue settings
	queue_table: str = 'v1_queue'
	queue_position_table: str = 'v1_queue_positions'
	concurrent_tasks: int = 2
	task_retry_delay: int = 60

	@property
	def base_path(self) -> Path:
		path = Path(self.base_dir)
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path

	@property
	def archive_path(self) -> Path:
		path = self.base_path / self.archive_dir
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path

	@property
	def cog_path(self) -> Path:
		path = self.base_path / self.cog_dir
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path

	@property
	def thumbnail_path(self) -> Path:
		path = self.base_path / self.thumbnails_dir
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path

	@property
	def user_label_path(self) -> Path:
		path = self.base_path / self.label_objects_dir
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path


settings = Settings()
