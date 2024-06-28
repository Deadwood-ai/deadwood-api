import platform
from fastapi import APIRouter, Request
from pydantic import BaseModel


class InfoResponse(BaseModel):
    name: str
    description: str
    system: dict
    endpoints: list[dict]



router = APIRouter()

@router.get("/", response_model=InfoResponse)
def info(request: Request):
    """
    Get information about the storage API.
    """
    # get the host and root path from the request
    scheme = request.scope.get("scheme")
    host = request.headers.get("host")
    root_path = request.scope.get("root_path")
    url = f"{scheme}://{host}{root_path}".strip('/')

    # create the info about the API server
    info = dict(
        name="Deadwood-AI Storage API.",
        description="This is the Deadwood-AI Storage API. It is used to manage files uploads for the Deadwood-AI backend. If you are not a developer, you may be searching for https://deadtrees.earth",
        system=dict(
            python_version=platform.python_version(),
            platform=platform.platform(),
            host=host,
            root_path=root_path,
            scopes=list(request.scope.keys()),
            server=request.scope.get("scheme"),
        ),
        endpoints=[
            dict(url=f"{url}/", description="Get information about the storage API."),
            dict(url=f"{url}/upload", description="Upload a file to the server."),
            dict(url=f"{url}/docs", description="OpenAPI documentation - Swagger UI."),
            dict(url=f"{url}/redoc", description="OpenAPI documentation - ReDoc."),
        ],
    )

    return info