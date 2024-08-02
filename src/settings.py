from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from pathlib import Path

# load an .env file if it exists
load_dotenv()


BASE = Path(__file__).parent.parent / "data"


# load the settings from environment variables
class Settings(BaseSettings):
    # base directory for the storage app
    base_dir: str = str(BASE)

    # directly specify the locations for several files
    archive_dir: str = "archive"
    cog_dir: str = "cogs"
    thumbnail_bucket: str = "v1_thumbnails"
    tmp_dir: str = "tmp"

    # supabase settings for supabase authentication
    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None

    # some basic settings for the UVICORN server
    uvicorn_host: str = "127.0.0.1"
    uvicorn_port: int = 8000
    uvicorn_root_path: str = "/"
    uvicorn_proxy_headers: bool = True

    # supabase settings
    processor_username: str = "processor@deadtrees.earth"
    processor_password: str = "processor"

    # tabe names
    datasets_table: str = "v1_datasets"
    metadata_table: str = "v1_metadata"
    cogs_table: str = "v1_cogs"
    labels_table: str = "v1_labels"

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
    def tmp_path(self) -> Path:
        path = self.base_path / self.tmp_dir
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

        return path


settings = Settings()
