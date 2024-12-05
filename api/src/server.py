from fastapi import FastAPI, Response
from starlette.middleware.cors import CORSMiddleware

from shared import monitoring
import logfire

# TODO: refactor this
import prometheus_client

from shared.__version__ import __version__
from .routers import process, upload, info, auth, labels, download, metadata

app = FastAPI(
	title='Deadwood-AI API',
	description='This is the Deadwood-AI API. It is used to manage files uploads to the Deadwood-AI backend and the preprocessing of uploads. Note that the download is managed by a sub-application at `/download/`.',
	version=__version__,
)

logfire.instrument_fastapi(app)

# add CORS middleware
app.add_middleware(
	CORSMiddleware,
	allow_origins=['https://deadtrees.earth', 'https://www.deadtrees.earth'],
	allow_origin_regex='https://deadwood-d4a4b.*|http://(127\\.0\\.0\\.1|localhost)(:\\d+)?',
	allow_credentials=True,
	allow_methods=['OPTIONS', 'GET', 'POST', 'PUT'],
	allow_headers=['Content-Type', 'Authorization', 'Origin', 'Accept'],
)


# add the prometheus metrics route
@app.get('/metrics')
def get_metrics():
	"""
	Supplys Prometheus metrics for the storage API.
	"""
	return Response(prometheus_client.generate_latest(), media_type='text/plain')


# add the info route to the app
app.include_router(info.router)

# add the upload route to the app
app.include_router(upload.router)

# add the metadata route to the app
app.include_router(metadata.router)

# add the auth rout to the app
app.include_router(auth.router)

# add the processing to the app
app.include_router(process.router)

# add the labels to the app
app.include_router(labels.router)

# add thumbnail route to the app
# app.include_router(thumbnail.router)


# add the download routes to the app
# app.include_router(download.download_app)
app.mount('/download', download.download_app)
