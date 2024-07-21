from typing import Annotated

from pydantic import BaseModel
from pydantic_geojson import MultiPolygonModel, PolygonModel
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from ..supabase import verify_token, use_client
from ..settings import settings
from ..logger import logger
from ..models import Dataset, Label
from ..deadwood.labels import verify_labels

# create the router for the labels
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# create a model fot the user input
class UserLabelInput(BaseModel):
    aoi: PolygonModel
    label: MultiPolygonModel
    source: str
    quality: str


@router.post("/{dataset_id}/labels")
def create_new_labels(dataset_id: int, data: UserLabelInput, token: Annotated[str, Depends(oauth2_scheme)]):
    """
    """
    # first thing we do is verify the token
    user = verify_token(token)
    if not user:
        return HTTPException(status_code=401, detail="Invalid token")
    
    # load the the dataset info for this one
    try:
        with use_client(token) as client:
            # filter using the given dataset_id
            response = client.table(settings.datasets_table).select('*').eq('id', dataset_id).execute()
            
            # create the dataset
            dataset = Dataset(**response.data[0])
    except Exception as e:
        # log the error to the database
        msg = f"Error loading dataset {dataset_id}: {str(e)}"
        logger.error(msg, extra={"token": token})
        
        return HTTPException(status_code=500, detail=msg)
    
    # verify the data
    try:
        verify_labels(data.aoi, data.label)
    except Exception as e:
        # log the error to the database
        msg = f"Invalid label data: {str(e)}"
        logger.error(msg, extra={"token": token})

        return HTTPException(status_code=400, detail=msg)

    # fill the metadata
    meta = dict(
        dataset_id=dataset.id,
        user_id=user.id,
        aoi=data.aoi,
        label=data.label,
        label_source=data.source,
        label_quality=data.quality
    )

    # dev
    print(meta)
    try:
        label = Label(**meta)
    except Exception as e:
        # log the error to the database
        msg = f"Error creating label object: {str(e)}"
        logger.error(msg, extra={"token": token})

        return HTTPException(status_code=400, detail=msg)

    # upload the dataset
    with use_client(token) as client:
        try:
            send_data = {k: v for k, v in label.model_dump().items() if k != 'id' and v is not None}
            response = client.table(settings.labels_table).insert(send_data).execute()
        except Exception as e:
            msg = f"An error occurred while trying to upload the label: {str(e)}"

            # log the error to the database
            logger.error(msg, extra={"token": token, dataset_id: dataset.id})
            return HTTPException(status_code=400, detail=msg)
    
    return label

