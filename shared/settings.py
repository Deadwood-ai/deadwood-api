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
	BASE_DIR: str = str(BASE)
	GADM_DATA_PATH: str = ''
	DEV_MODE: bool = False

	# directly specify the locations for several files
	ARCHIVE_DIR: str = 'archive'
	COG_DIR: str = 'cogs'
	THUMBNAIL_DIR: str = 'thumbnails'
	LABEL_OBJECTS_DIR: str = 'label_objects'

	# Temporary processing directory
	# tmp_processing_path: str = str(Path(tempfile.mkdtemp(prefix='processing')))

	# supabase settings for supabase authentication
	SUPABASE_URL: Optional[str] = None
	SUPABASE_KEY: Optional[str] = None

	# some basic settings for the UVICORN server
	UVICORN_HOST: str = '127.0.0.1'
	UVICORN_PORT: int = 8000
	UVICORN_ROOT_PATH: str = ''
	UVICORN_PROXY_HEADERS: bool = True

	# storage server settings
	STORAGE_SERVER_IP: str = ''
	STORAGE_SERVER_USERNAME: str = ''
	STORAGE_SERVER_DATA_PATH: str = ''

	# api endpoint
	API_ENDPOINT: str = 'https://data.deadtrees.earth/api/v1/'
	API_ENTPOINT_DATASETS: str = API_ENDPOINT + 'datasets/'

	# processor settings
	PROCESSOR_USERNAME: str = 'processor@deadtrees.earth'
	PROCESSOR_PASSWORD: str = 'processor'
	SSH_PRIVATE_KEY_PATH: str = '/app/ssh_key'
	SSH_PRIVATE_KEY_PASSPHRASE: str = ''

	processing_dir: str = 'processing'

	_PROCESSING_PATH: Optional[Path] = None

	# monitoring
	LOGFIRE_TOKEN: str = None
	LOGFIRE_PYDANTIC_PLUGIN_RECORD: str = 'all'

	@property
	def processing_path(self) -> Path:
		if self._PROCESSING_PATH is None:
			self._PROCESSING_PATH = Path(tempfile.mkdtemp(prefix='processing_'))
		return self._PROCESSING_PATH

	@property
	def base_path(self) -> Path:
		path = Path(self.BASE_DIR)
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path

	@property
	def archive_path(self) -> Path:
		path = self.base_path / self.ARCHIVE_DIR
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path

	@property
	def cog_path(self) -> Path:
		path = self.base_path / self.COG_DIR
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path

	@property
	def thumbnail_path(self) -> Path:
		path = self.base_path / self.THUMBNAIL_DIR
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path

	@property
	def user_label_path(self) -> Path:
		path = self.base_path / self.LABEL_OBJECTS_DIR
		if not path.exists():
			path.mkdir(parents=True, exist_ok=True)

		return path

	@property
	def _tables(self) -> dict:
		return _dev_tables if self.DEV_MODE else _production_tables

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
