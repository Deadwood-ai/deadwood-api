from typing import Annotated
import uuid
from pathlib import Path
import time
import hashlib
import rasterio

from fastapi import APIRouter, UploadFile, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from ..models import Metadata, MetadataPayloadData, Dataset, StatusEnum
from ..supabase import use_client, verify_token
from ..settings import settings
from ..logger import logger
from ..deadwood.osm import get_admin_tags
from  .. import monitoring

# create the router for the upload
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# little helper
def format_size(size: int) -> str:
    """Converting the filesize of the geotiff into a human readable format for the logger

    Args:
        size (int): File size in bytes

    Returns:
        str: A proper human readable size string in bytes, KB, MB or GB
    """
    if size < 1024:
        return f"{size} bytes"
    elif size < 1024**2:
        return f"{size / 1024:.2f} KB"
    elif size < 1024**3:
        return f"{size / 1024**2:.2f} MB"
    else:
        return f"{size / 1024**3:.2f} GB"

# Main routes for the logic
@router.post("/datasets")
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
    # count an invoke
    monitoring.uploads_invoked.inc()

    # first thing we do is verify the token
    user = verify_token(token)
    if not user:
        return HTTPException(status_code=401, detail="Invalid token")
    
    # we create a uuid for this dataset
    uid = str(uuid.uuid4())

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
        transformed_bounds = rasterio.warp.transform_bounds(src.crs, 'EPSG:4326', *bounds)
    
    # stop the timer
    t2 = time.time()

    # fill the metadata
    #dataset = Dataset(
    data = dict(
        file_name=target_path.name,
        file_alias=file.filename,
        file_size=target_path.stat().st_size,
        copy_time=t2 - t1,
        sha256=sha256,
        #bbox=f"BOX({bounds.bottom} {bounds.left}, {bounds.top} {bounds.right})",
        bbox=transformed_bounds,
        status=StatusEnum.pending,
        user_id=user.id
    )
    # print(data)
    dataset=Dataset(**data)

    # upload the dataset
    with use_client(token) as client:
        try:
            send_data = {k: v for k, v in dataset.model_dump().items() if k != 'id' and v is not None}
            response = client.table(settings.datasets_table).insert(send_data).execute()
        except Exception as e:
            logger.exception(f"An error occurred while trying to upload the dataset: {str(e)}", extra={"token": token, "user_id": user.id})
            raise HTTPException(status_code=400, detail=f"An error occurred while trying to upload the dataset: {str(e)}")
    
    # update the dataset with the id
    dataset = Dataset(**response.data[0])
    
    # do some monitoring
    monitoring.uploads_counter.inc()
    monitoring.upload_time.observe(dataset.copy_time)
    monitoring.upload_size.observe(dataset.file_size)

    logger.info(
        f"Created new dataset <ID={dataset.id}> with file {dataset.file_alias}. ({format_size(dataset.file_size)}). Took {dataset.copy_time:.2f}s.", 
        extra={"token": token, "user_id": user.id, "dataset_id": dataset.id}
    )

    return dataset


@router.put("/datasets/{dataset_id}/metadata")
def upsert_metadata(dataset_id: int, payload: MetadataPayloadData, token: Annotated[str, Depends(oauth2_scheme)]):
    """
    Insert or Update the metadata of a Dataset.

    Right now, the API requires that always a valid Metadata instance is sent. 
    Thus, the frontend can change the values and send the whole Metadata object.
    The token needs to include the access token of the user that is allowed to change the metadata.

    """
    # count an invoke
    monitoring.metadata_invoked.inc()

    # first thing we do is verify the token
    user = verify_token(token)
    if not user:
        return HTTPException(status_code=401, detail="Invalid token")

    # load the metadata info - if it already exists in the database
    with use_client(token) as client:
        response = client.table(settings.metadata_table).select('*').eq('dataset_id', dataset_id).execute()

        if len(response.data) > 0:
            metadata = Metadata(**response.data[0]).model_dump()
        else:
            logger.info(f"No existing Metadata found for Dataset {dataset_id}. Creating a new one.", extra={"token": token, "dataset_id": dataset_id, "user_id": user.id})
            metadata = {'dataset_id': dataset_id, 'user_id': user.id}

    # update the given metadata if any with the payload
    try:
        metadata.update(**{k: v for k, v in payload.model_dump().items() if v is not None})
        metadata = Metadata(**metadata)
    except Exception as e:
        msg = f"An error occurred while trying to create the updated metadata: {str(e)}"

        logger.exception(msg, extra={"token": token, "dataset_id": dataset_id, "user_id": user.id})
        return HTTPException(status_code=400, detail=msg)

    # if the metadata does not have admin level names, query them from OSM
    if metadata.admin_level_1 is None:
        # get the bounding box
        try:
            with use_client(token) as client:
                response = client.table(settings.datasets_table).select('*').eq('id', dataset_id).execute()
                data = Dataset(**response.data[0])

                # get the tags of the centroid
                (lvl1, lvl2, lvl3) = get_admin_tags(data.centroid)

                # update the metadata model
                metadata.admin_level_1 = lvl1
                metadata.admin_level_2 = lvl2
                metadata.admin_level_3 = lvl3

        except Exception as e:
            msg = f"An error occurred while querying OSM for admin level names of dataset_id: {dataset_id}: {str(e)}"
            logger.error(msg, extra={"token": token, "dataset_id": dataset_id, "user_id": user.id})
        
    try:
        # upsert the given metadata entry with the merged data
        with use_client(token) as client:
            send_data = {k: v for k, v in metadata.model_dump().items() if v is not None}
            response = client.table(settings.metadata_table).upsert(send_data).execute()
    except Exception as e:
        err_msg = f"An error occurred while trying to upsert the metadata of Dataset <ID={dataset_id}>: {e}"
        
        # log the error to the database
        logger.error(err_msg, extra={"token": token, "dataset_id": dataset_id, "user_id": user.id})

        # return a response with the error message
        return HTTPException(
            status_code=400,
            detail=err_msg
        )

    # no error occured, so return the upserted metadata
    logger.info(f"Upserted metadata for Dataset {dataset_id}. Upsert payload provided by user: {payload}", extra={"token": token, "dataset_id": dataset_id, "user_id": user.id})
    
    # update the metadata
    metadata = Metadata(**response.data[0])
    monitoring.metadata_counter.inc()

    return metadata
