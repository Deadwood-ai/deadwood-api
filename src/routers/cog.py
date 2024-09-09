from typing import Optional, Annotated
from pathlib import Path
import time

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer

from ..supabase import verify_token, use_client
from ..settings import settings
from ..models import Dataset, ProcessOptions, TaskPayload, QueueTask, StatusEnum, Cog
from ..logger import logger
from .. import monitoring
from ..queue import background_process

from ..processing import update_status
from ..deadwood.cog import calculate_cog

# create the router for the processing
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@router.put("/datasets/{dataset_id}/force-cog-build")
async def create_direct_cog(dataset_id: int, options: Optional[ProcessOptions], token: Annotated[str, Depends(oauth2_scheme)]):
    """
    This route will bypass the queue and directly start the cog calculation for the given dataset_id.
    """
    # count an invoke
    monitoring.cog_invoked.inc()
    pass

    # first thing we do is verify the token
    user = verify_token(token)
    if not user:
        return HTTPException(status_code=401, detail="Invalid token")
    
    # load the dataset
    try:
        with use_client(token) as client:
            response = client.table(settings.datasets_table).select('*').eq('id', dataset_id).execute()
            dataset = Dataset(**response.data[0])
    except Exception as e:
        # log the error to the database
        msg = f"Error loading dataset {dataset_id}: {str(e)}"
        logger.error(msg, extra={"token": token, "user_id": user.id, "dataset_id": dataset_id})
        
        return HTTPException(status_code=500, detail=msg)
    
    # if we are still here, update the status to processing
    update_status(token, dataset.id, StatusEnum.processing)

    # get the output path settings
    cog_folder = settings.cog_path / Path(dataset.file_name).stem
    file_name = f"{cog_folder}_cog_{options.profile}_ovr{options.overviews}_q{options.quality}.tif"

    # output path is the cog folder, then a folder for the dataset, then the cog file
    output_path = settings.cog_path / cog_folder / file_name

    # get the input path
    input_path = settings.archive_path / dataset.file_name

    # crete if not exists
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    # start the cog calculation
    t1 = time.time()
    try:
        info = calculate_cog(
            str(input_path), 
            str(output_path), 
            profile=options.profile, 
            overviews=options.overviews, 
            quality=options.quality,
            skip_recreate=not options.force_recreate
        )
        logger.info(f"COG profile returned for dataset {dataset.id}: {info}", extra={"token": token, "dataset_id": dataset.id, "user_id": user.id})
    except Exception as e:
        msg = f"Error processing COG for dataset {dataset.id}: {str(e)}"

        # set the status
        update_status(token, dataset.id, StatusEnum.errored)

        # log the error to the database
        logger.error(msg, extra={"token": token, "user_id": user.id, "dataset_id": dataset.id})
        return
    
    # stop the timer
    t2 = time.time()

    # fill the metadata
    meta = dict(
        dataset_id=dataset.id,
        cog_folder=str(cog_folder),
        cog_name=file_name,
        cog_url=f"{cog_folder}/{file_name}",
        cog_size=output_path.stat().st_size,
        runtime=t2 - t1,
        user_id=user.id,
        compression=options.profile,
        overviews=options.overviews,
        tiling_scheme=options.tiling_scheme,
        # !! This is not correct!! 
        resolution=int(options.resolution * 100),
        blocksize=info.IFD[0].Blocksize[0],
    )

    # Build the Cog metadata
    cog = Cog(**meta)

    with use_client(token) as client:
        try:
            # filter out the None data
            send_data = {k: v for k, v in cog.model_dump().items() if v is not None}
            response = client.table(settings.cogs_table).upsert(send_data).execute()
        except Exception as e:
            msg = f"An error occured while trying to save the COG metadata for dataset {dataset.id}: {str(e)}"

            logger.error(msg, extra={"token": token, "user_id": user.id, "dataset_id": dataset.id})
            update_status(token, dataset.id, StatusEnum.errored)
    
    # if there was no error, update the status
    update_status(token, dataset.id, StatusEnum.processed)

    logger.info(f"Finished creating new COG <profile: {cog.compression}> for dataset {dataset.id}.", extra={"token": token, "dataset_id": dataset.id, "user_id": user.id})





    
@router.put("/datasets/{dataset_id}/build-cog")
def create_cog(dataset_id: int, options: Optional[ProcessOptions], token: Annotated[str, Depends(oauth2_scheme)], background_tasks: BackgroundTasks):
    """FastAPI process chain to add a cog-calculation task to the processing queue, with monitoring and logging.
    Verifies the access token, loads the dataset to calculate, creates a TaskPayload and adds the task to 
    the background process of FastAPI. The task metadata is returned to inform the user on the frontend 
    about the queue position and estimated wait time.

    Args:
        dataset_id (int): The id of the processed cog
        options (Optional[ProcessOptions]): Optional processsing options to change the standard settings for the cog creation
        token (Annotated[str, Depends): Supabase access token
        background_tasks (BackgroundTasks): FastAPI background tasks object

    Returns:
        QueueTask: Returns the task
    """
    # count an invoke
    monitoring.cog_invoked.inc()

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
        logger.error(msg, extra={"token": token, "user_id": user.id, "dataset_id": dataset_id})
        
        return HTTPException(status_code=500, detail=msg)

    # get the options
    options = options or ProcessOptions()

    # add a new task to the queue
    try:
        payload = TaskPayload(
            dataset_id=dataset.id,
            user_id=user.id,
            build_args=options,
            priority=2,
            is_processing=False
        )

        with use_client(token) as client:
            send_data = {k: v for k, v in payload.model_dump().items() if v is not None and k != 'id'}
            response = client.table(settings.queue_table).insert(send_data).execute()
            payload = TaskPayload(**response.data[0])
    except Exception as e:
        # log the error to the database
        msg = f"Error adding task to queue: {str(e)}"
        logger.error(msg, extra={"token": token, "user_id": user.id, "dataset_id": dataset_id})
        
        return HTTPException(status_code=500, detail=msg)
        
    # load the current position assigned to this task
    try:
        with use_client(token) as client:
            response = (
                client.table(settings.queue_position_table)
                .select('*')
                .eq('id', payload.id)
                .execute()
            )
            task = QueueTask(**response.data[0])
    except Exception as e:
        # log the error to the database
        msg = f"Error loading task position: {str(e)}"
        logger.error(msg, extra={"token": token, "user_id": user.id, "dataset_id": dataset_id})
        
        return HTTPException(status_code=500, detail=msg)
    
    # start the background task
    background_tasks.add_task(background_process)

    # return the task
    return task



