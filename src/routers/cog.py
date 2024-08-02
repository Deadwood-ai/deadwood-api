from typing import Optional, Annotated

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer

from ..supabase import verify_token, use_client
from ..settings import settings
from ..models import Dataset, ProcessOptions, TaskPayload, QueueTask
from ..logger import logger
from .. import monitoring
from ..queue import background_process


# create the router for the processing
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


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



