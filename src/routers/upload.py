from typing import Annotated

from fastapi import APIRouter, UploadFile, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from ..models import Metadata
from ..supabase import use_client
from ..settings import settings
from ..logger import logger

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
def upsert_metadata(dataset_id: str, metadata: Metadata, token: Annotated[str, Depends(oauth2_scheme)]):
    """
    Insert or Update the metadata of a Dataset.

    Right now, the API requires that always a valid Metadata instance is sent. 
    Thus, the frontend can change the values and send the whole Metadata object.
    The token needs to include the access token of the user that is allowed to change the metadata.

    """
    # update the given metadata  with the dataset_id
    metadata.dataset_id = dataset_id
    try:
        # upsert the given metadata entry
        with use_client(token) as client:
            response = client.table(settings.metadata_table).upsert(metadata.model_dump()).execute()
    except Exception as e:
        err_msg = f"An error occurred while trying to upsert the metadata: {e}"
        
        # log the error to the database
        logger.error(err_msg)

        # return a response with the error message
        return HTTPException(
            status_code=400,
            detail=err_msg
        )

    # no error occured, so return the upserted metadata
    return {
        "dataset_id": dataset_id,
        "metadata ": response.data,
        "message": f"Dataset {dataset_id} updated."
    }


# add function to add the labels in similar fashion

# add GET requests to read the data without the need for the token