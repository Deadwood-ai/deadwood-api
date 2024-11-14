import uvicorn

from shared.settings import settings

# from src.processor.src.queue import background_process


def run(
	host: str = settings.uvicorn_host,
	port: int = settings.uvicorn_port,
	root_path: str = settings.uvicorn_root_path,
	proxy_headers: bool = settings.uvicorn_proxy_headers,
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
