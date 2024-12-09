from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from pathlib import Path
import tempfile

# load an .env file if it exists
load_dotenv()


_production_tables = {
	'datasets': 'v1_datasets',
	'metadata': 'v1_metadata',
	'cogs': 'v1_cogs',
	'labels': 'v1_labels',
	'thumbnails': 'v1_thumbnails',
	'logs': 'logs',
	'label_objects': 'v1_label_objects',
	'queue': 'v1_queue',
	'queue_positions': 'v1_queue_positions',
}

_dev_tables = {
	'datasets': 'dev_datasets',
	'metadata': 'dev_metadata',
	'cogs': 'dev_cogs',
	'labels': 'dev_labels',
	'thumbnails': 'dev_thumbnails',
	'logs': 'dev_logs',
	'label_objects': 'dev_label_objects',
	'queue': 'dev_queue',
	'queue_positions': 'dev_queue_positions',
}


BASE = Path(__file__).parent.parent / 'data'


# load the settings from environment variables
class Settings(BaseSettings):
	# base directory for the storage app
	base_dir: str = str(BASE)

	dev_mode: bool = False

	# directly specify the locations for several files
	archive_dir: str = 'archive'
	cog_dir: str = 'cogs'
	thumbnails_dir: str = 'thumbnails'
	label_objects_dir: str = 'label_objects'

	# Temporary processing directory
	# tmp_processing_path: str = str(Path(tempfile.mkdtemp(prefix='processing')))

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

	processing_dir: str = 'processing'

	_processing_path: Optional[Path] = None

	# monitoring
	LOGFIRE_TOKEN: str = None
	LOGFIRE_PYDANTIC_PLUGIN_RECORD: str = 'all'

	@property
	def processing_path(self) -> Path:
		if self._processing_path is None:
			self._processing_path = Path(tempfile.mkdtemp(prefix='processing_'))
		return self._processing_path

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

	@property
	def _tables(self) -> dict:
		return _dev_tables if self.dev_mode else _production_tables

	@property
	def datasets_table(self) -> str:
		return self._tables['datasets']

	@property
	def metadata_table(self) -> str:
		return self._tables['metadata']

	@property
	def cogs_table(self) -> str:
		return self._tables['cogs']

	@property
	def labels_table(self) -> str:
		return self._tables['labels']

	@property
	def thumbnails_table(self) -> str:
		return self._tables['thumbnails']

	@property
	def logs_table(self) -> str:
		return self._tables['logs']

	@property
	def label_objects_table(self) -> str:
		return self._tables['label_objects']

	@property
	def queue_table(self) -> str:
		return self._tables['queue']

	@property
	def queue_position_table(self) -> str:
		return self._tables['queue_positions']


settings = Settings()
