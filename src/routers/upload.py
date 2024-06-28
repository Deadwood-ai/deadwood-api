from typing import Annotated

from fastapi import APIRouter, UploadFile, Depends
from fastapi.security import OAuth2PasswordBearer

from ..models import Metadata

# create the router for the upload
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Main routes for the logic
@router.post("/create")
def upload_geotiff(file: UploadFile, token: Annotated[str, Depends(oauth2_scheme)]):
    """
    Create a new Dataset by uploading a GeoTIFF file.

    Further metadata is not yet necessary. The response will contain a Dataset.id 
    that is needed for subsequent calls to the API. Once, the GeoTIFF is uploaded, 
    the backend will start pre-processing the file. 
    It can only be used in the front-end once preprocessing finished AND all mandatory
    metadata is set.

    To send the file use the `multipart/form-data` content type. The file has to be sent as the 
    value of a field named `file`. For example, using HTML forms like this:

    ```html
    <form action="/upload" method="post" enctype="multipart/form-data">
        <input type="file" name="file">
        <input type="submit">
    </form>
    ```

    Or using the `requests` library in Python like this:

    ```python
    import requests
    url = "http://localhost:8000/upload"
    files = {"file": open("example.txt", "rb")}
    response = requests.post(url, files=files)
    print(response.json())
    ```
    """
    return {
        "file_name": file.filename, 
        "token": token, 
        "message": "THIS IS NOT YET IMPLEMENTED. This route will create a new Dataset by uploading a GeoTIFF file. The response will contain a Dataset.id that is needed for subsequent calls to the API."
    }


@router.post("/{dataset_id}/metadata")
def usert_metadata(dataset_id: str, metadata: Metadata, token: Annotated[str, Depends(oauth2_scheme)]):
    """
    Insert or Update the metadata of a Dataset.

    Right now, the API requires that always a valid Metadata instance is sent. 
    Thus, the frontend can change the values and send the whole Metadata object.
    The token needs to include the access token of the user that is allowed to change the metadata.

    """
    return {
        "dataset_id": dataset_id,
        "metadata": metadata.model_dump_json(),
        "token": token,
        "message": "THIS IS NOT YET IMPLEMENTED. This route will insert or update the metadata of a Dataset."
    }

# add function to add the labels in similar fashion

# add GET requests to read the data without the need for the token