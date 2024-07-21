from typing import Optional, Annotated
from pathlib import Path
import time

from pydantic_settings import BaseSettings
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from ..supabase import verify_token, use_client
from ..settings import settings
from ..models import Dataset, Cog, StatusEnum
from ..logger import logger
from ..deadwood.cog import calculate_cog


# create the router for the processing
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def update_status(token: str, dataset_id: int, status: StatusEnum):
    with use_client(token) as client:
        client.table(settings.datasets_table).update({
            'status': status.value,
        }).eq('id', dataset_id).execute()


class ProcessOptions(BaseSettings):
    overviews: Optional[int] = 8
    resolution: Optional[float] = 0.04
    profile: Optional[str] = "webp"
    force_recreate: Optional[bool] = False


@router.put("/datasets/{dataset_id}/build-cog")
def create_cog(dataset_id: int, options: Optional[ProcessOptions], token: Annotated[str, Depends(oauth2_scheme)]):
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

    # from here on we have a dataset object
    update_status(token, dataset.id, StatusEnum.processing)

    # get the input path
    input_path = settings.archive_path / dataset.file_name

    # get the options
    options = options or ProcessOptions()

    # get the output settings
    cog_folder = Path(dataset.file_name).stem
    file_name = f"{cog_folder}_cog_{options.profile}_ovr{options.overviews}.tif"

    # output path is the cog folder, then a folder for the dataset, then the cog file
    output_path = settings.cog_path / cog_folder / file_name

    # crete if not exists
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    # start the timer
    t1 = time.time()
    try:
        info = calculate_cog(str(input_path), str(output_path), options.profile, options.overviews, skip_recreate=not options.force_recreate)
        print(info)
    except Exception as e:
        msg = f"Error processing COG for dataset {dataset_id}: {str(e)}"

        # set the status
        update_status(token, dataset.id, StatusEnum.errored)

        # log the error to the database
        logger.error(msg, extra={"token": token})
        return HTTPException(status_code=500, detail=msg)
    
    # get the size of the output file
    pass
    # stop the timer
    t2 = time.time()

    # fill the metadata
    meta = dict(
        dataset_id=dataset_id,
        cog_folder=cog_folder,
        cog_name=file_name,
        cog_url=f"{cog_folder}/{file_name}",
        cog_size=output_path.stat().st_size,
        runtime=t2 - t1,
        user_id=user.id,
        compression=options.profile,
        overviews=options.overviews,
        # !! This is not correct!! 
        resolution=int(options.resolution * 100),
        blocksize=info.IFD[0].Blocksize[0],
    )

    # dev
    print(meta)
    cog = Cog(**meta)

    with use_client(token) as client:
        try:
            # filter out the None data
            send_data = {k: v for k, v in cog.model_dump().items() if k != 'id' and v is not None}
            response = client.table(settings.cogs_table).insert(send_data).execute()
        except Exception as e:
            msg = f"An error occured while trying to save the COG metadata for dataset {dataset_id}: {str(e)}"

            logger.error(msg, extra={"token": token})
            update_status(token, dataset.id, StatusEnum.errored)
            return HTTPException(status_code=500, detail=msg)
    
    # if there was no error, update the status
    update_status(token, dataset.id, StatusEnum.processed)

    return cog
