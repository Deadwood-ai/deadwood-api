from pathlib import Path
import time

from .supabase import use_client, login
from .settings import settings
from .models import StatusEnum, Dataset, QueueTask, Cog
from .logger import logger
from . import monitoring
from .deadwood.cog import calculate_cog


def update_status(token: str, dataset_id: int, status: StatusEnum):
    with use_client(token) as client:
        client.table(settings.datasets_table).update({
            'status': status.value,
        }).eq('id', dataset_id).execute()


def process_cog(task: QueueTask):
    # login with the processor
    token = login(settings.processor_username, settings.processor_password).session.access_token

    # load the dataset
    try:
        with use_client(token) as client:
            # filter using the given dataset_id
            response = client.table(settings.datasets_table).select('*').eq('id', task.dataset_id).execute()
            
            # create the dataset
            dataset = Dataset(**response.data[0])
    except Exception as e:
        # log the error to the database
        msg = f"PROCESSOR error loading dataset {task.dataset_id}: {str(e)}"
        logger.error(msg, extra={"token": token, "user_id": task.user_id, "dataset_id": task.dataset_id})
    
    # update the status to processing
    update_status(token, dataset_id=dataset.id, status=StatusEnum.processing)

    # get the input path
    input_path = settings.archive_path / dataset.file_name

    # get the options
    options = task.build_args

    # get the output settings
    cog_folder = Path(dataset.file_name).stem
    file_name = f"{cog_folder}_cog_{options.profile}_ovr{options.overviews}_q{options.quality}.tif"

    # output path is the cog folder, then a folder for the dataset, then the cog file
    output_path = settings.cog_path / cog_folder / file_name

    # crete if not exists
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True, exist_ok=True)

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
        logger.info(f"COG profile returned for dataset {dataset.id}: {info}", extra={"token": token, "dataset_id": dataset.id, "user_id": task.user_id})
    except Exception as e:
        msg = f"Error processing COG for dataset {dataset.id}: {str(e)}"

        # set the status
        update_status(token, dataset.id, StatusEnum.errored)

        # log the error to the database
        logger.error(msg, extra={"token": token, "user_id": task.user_id, "dataset_id": dataset.id})
        return
    
    # get the size of the output file
    pass
    # stop the timer
    t2 = time.time()

    # fill the metadata
    meta = dict(
        dataset_id=dataset.id,
        cog_folder=cog_folder,
        cog_name=file_name,
        cog_url=f"{cog_folder}/{file_name}",
        cog_size=output_path.stat().st_size,
        runtime=t2 - t1,
        user_id=task.user_id,
        compression=options.profile,
        overviews=options.overviews,
        # !! This is not correct!! 
        resolution=int(options.resolution * 100),
        blocksize=info.IFD[0].Blocksize[0],
    )

    # dev
    cog = Cog(**meta)

    with use_client(token) as client:
        try:
            # filter out the None data
            send_data = {k: v for k, v in cog.model_dump().items() if k != 'id' and v is not None}
            response = client.table(settings.cogs_table).insert(send_data).execute()
        except Exception as e:
            msg = f"An error occured while trying to save the COG metadata for dataset {dataset.id}: {str(e)}"

            logger.error(msg, extra={"token": token, "user_id": task.user_id, "dataset_id": dataset.id})
            update_status(token, dataset.id, StatusEnum.errored)
    
    # if there was no error, update the status
    update_status(token, dataset.id, StatusEnum.processed)

    # monitoring
    monitoring.cog_counter.inc()
    monitoring.cog_time.observe(cog.runtime)
    monitoring.cog_size.observe(cog.cog_size)

    logger.info(f"Finished creating new COG <profile: {cog.compression}> for dataset {dataset.id}.", extra={"token": token, "dataset_id": dataset.id, "user_id": task.user_id})


