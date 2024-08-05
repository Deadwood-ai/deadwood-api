from fastapi import FastAPI, Response
from starlette.middleware.cors import CORSMiddleware


import prometheus_client

from .__version__ import __version__
from .routers import cog, upload, info, auth, labels

app = FastAPI(
    title="Deadwood-AI API",
    description="This is the Deadwood-AI API. It is used to manage files uploads to the Deadwood-AI backend and the preprocessing of uploads.",
    version=__version__,
)

# add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=['https://deadtrees.earth', 'https://www.deadtrees.earth'],
    allow_origin_regex='http://localhost:.*',
    allow_credentials=True,
    allow_methods=['OPTIONS, GET, POST, PUT'],
    allow_headers=['Content-Type', 'Authorization', 'Origin', 'Accept'],
)

# add the info route to the app
app.include_router(info.router)

# add the upload route to the app
app.include_router(upload.router)

# add the auth rout to the app
app.include_router(auth.router)

# add the processing to the app
app.include_router(cog.router)

# add the labels to the app
app.include_router(labels.router)


# add the prometheus metrics route
@app.get("/metrics")
def get_metrics():
    """
    Supplys Prometheus metrics for the storage API.
    """
    return Response(prometheus_client.generate_latest(), media_type="text/plain")
