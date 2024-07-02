from typing import Annotated
import uuid
from pathlib import Path
import time
import hashlib
import rasterio

from fastapi import APIRouter, UploadFile, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from ..models import Metadata, Dataset, StatusEnum
from ..supabase import use_client, verify_token
from ..settings import settings
from ..logger import logger

# create the router for the upload
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Main routes for the logic
@router.post("/create")
async def upload_geotiff(file: UploadFile, token: Annotated[str, Depends(oauth2_scheme)]):
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
    # first thing we do is verify the token
    user = verify_token(token)
    if not user:
        return HTTPException(status_code=401, detail="Invalid token")
    
    # we create a uuid for this dataset
    uid = str(uuid.uuid4)

    # new file name
    file_name = f"{uid}_{Path(file.filename).stem}.tif"

    # use the settings path to figure out a new location for this file
    target_path = settings.archive_path  / file_name

    # start a timer
    t1 = time.time()

    # save the file
    with target_path.open('wb') as buffer:
        buffer.write(await file.read())
    
    # create the checksum
    with target_path.open('rb') as f:
        sha256 = hashlib.sha256(f.read()).hexdigest()

    # try to open with rasterio
    with rasterio.open(str(target_path), 'r') as src:
        bounds = src.bounds
    
    # stop the timer
    t2 = time.time()

    # fill the metadata
    dataset = Dataset(
        file_name=target_path.name,
        file_alias=file.filename,
        file_size=target_path.stat().st_size,
        copy_time=t2 - t1,
        sha256=sha256,
        bbox=f"BOX({bounds.bottom} {bounds.left}, {bounds.top} {bounds.right})",
        stauts=StatusEnum.pending,
        user_id=user.id
    )

    # upload the dataset
    with use_client(token) as client:
        try:
            client.table(settings.datasets_table).insert(dataset.model_dump()).execute()
        except Exception as e:
            logger.exception(f"An error occurred while trying to upload the dataset: {str(e)}")
            raise HTTPException(status_code=400, detail=f"An error occurred while trying to upload the dataset: {str(e)}")
    

    return dataset


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