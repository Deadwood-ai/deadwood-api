from typing import Optional, Annotated
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from ..supabase import verify_token, use_client
from ..settings import settings
from ..deadwood.thumbnail import calculate_thumbnail
from ..models import Dataset, Cog, StatusEnum
from ..logger import logger


# create the router for the processing
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@router.put("/datasets/{dataset_id}/build-thumbnail")
def create_thumbnail(dataset_id: int, token: Annotated[str, Depends(oauth2_scheme)]):
    """ """
    # first thing we do is verify the token
    user = verify_token(token)
    if not user:
        return HTTPException(status_code=401, detail="Invalid token")

    # load the the dataset info for this one
    try:
        with use_client(token) as client:
            # filter using the given dataset_id
            response = (
                client.table(settings.datasets_table)
                .select("*")
                .eq("id", dataset_id)
                .execute()
            )

            # create the dataset
            dataset = Dataset(**response.data[0])
    except Exception as e:
        # log the error to the database
        msg = f"Error loading dataset {dataset_id}: {str(e)}"
        logger.error(msg, extra={"token": token})

        return HTTPException(status_code=500, detail=msg)

    # get the file path
    tiff_file_path = Path(settings.archive_path) / dataset.file_name

    # create the thumbnail
    thumbnail_target_path = Path(settings.thumbnail_path) / dataset.file_name.replace(
        ".tif", ".jpg"
    )
    # if the thumbnail already exists, remove it
    # this way we could recreate the thumbnail if needed, but up for discussion

    if thumbnail_target_path.exists():
        thumbnail_target_path.unlink()

    # create the thumbnail
    try:
        calculate_thumbnail(tiff_file_path, thumbnail_target_path)
    except Exception as e:
        # log the error to the database
        msg = f"Error creating thumbnail for dataset {dataset_id}: {str(e)}"
        logger.error(msg, extra={"token": token})

        return HTTPException(status_code=500, detail=msg)

    # update or adding url to either v1_datasets or v1_metadata
    # for now, not implemented. If made public via the webserver, I can create the URL in the frontend.
    # This is a placeholder for now.

    return {
        "message": "Thumbnail created successfully",
        "thumbnail_path": thumbnail_target_path,
    }
