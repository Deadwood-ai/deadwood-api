import uvicorn

from shared.settings import settings


def run(
	host: str = settings.UVICORN_HOST,
	port: int = settings.UVICORN_PORT,
	root_path: str = settings.UVICORN_ROOT_PATH,
	proxy_headers: bool = settings.UVICORN_PROXY_HEADERS,
	reload=False,
):
	"""
	Run the storage app using the uvicron wsgi server. the settings are loaded from the
	settings submodule, but can be overwritten with directly.
	:param host: host for the server
	:param port: port for the server
	:param root_path: root path for the server
	:param proxy_headers: use proxy headers
	:param reload: reload the server on file changes
	"""
	uvicorn.run(
		'api.src.server:app', host=host, port=port, root_path=root_path, proxy_headers=proxy_headers, reload=reload
	)


if __name__ == '__main__':
	from fire import Fire

	Fire({'server': run})
