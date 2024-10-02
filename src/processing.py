from pathlib import Path
import time
import paramiko

from .supabase import use_client, login
from .settings import settings
from .models import StatusEnum, Dataset, QueueTask, Cog
from .logger import logger
from . import monitoring
from .deadwood.cog import calculate_cog


def copy_pull_file_from_remote(remote_file_path: str, local_file_path: str):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        settings.processing_server_ip,
        settings.processing_server_username,
        settings.processing_server_password,
    )
    sftp = ssh.open_sftp()
    sftp.get(remote_file_path, local_file_path)
    sftp.close()
    ssh.close()


def copy_push_file_to_remote(local_file_path: str, remote_file_path: str):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        settings.processing_server_ip,
        settings.processing_server_username,
        settings.processing_server_password,
    )
    sftp = ssh.open_sftp()
    sftp.put(local_file_path, remote_file_path)
    sftp.close()
    ssh.close()


def update_status(token: str, dataset_id: int, status: StatusEnum):
    """Function to update the status field of a dataset about the cog calculation process.

    Args:
        token (str): Supabase client session token
        dataset_id (int): Unique id of the dataset
        status (StatusEnum): The current status of the cog calculation process to set the dataset to
    """
    with use_client(token) as client:
        client.table(settings.datasets_table).update(
            {
                "status": status.value,
            }
        ).eq("id", dataset_id).execute()


def process_cog(task: QueueTask):
    """Function to calculate a cloud optimized geotiff (cog) for the current QueueTask.
    Connects to the supabase metadata database, keeps the status up to date during the
    process, executes the calculate_cog function to calculate the cog and logs
    any potential errors during the process. In the end it will upload the cog and
    update prometheus to monitor the cog proccessing.

    Args:
        task (QueueTask): A QueueTask task containing all the required info for the cog processing
    """
    # login with the processor
    token = login(
        settings.processor_username, settings.processor_password
    ).session.access_token

    # load the dataset
    try:
        with use_client(token) as client:
            # filter using the given dataset_id
            response = (
                client.table(settings.datasets_table)
                .select("*")
                .eq("id", task.dataset_id)
                .execute()
            )

            # create the dataset
            dataset = Dataset(**response.data[0])
    except Exception as e:
        # log the error to the database
        msg = f"PROCESSOR error loading dataset {task.dataset_id}: {str(e)}"
        logger.error(
            msg,
            extra={
                "token": token,
                "user_id": task.user_id,
                "dataset_id": task.dataset_id,
            },
        )

    # update the status to processing
    update_status(token, dataset_id=dataset.id, status=StatusEnum.processing)

    # get the input path
    input_path = settings.archive_path / dataset.file_name

    # get the options
    options = task.build_args

    # get the output settings
    cog_folder = Path(dataset.file_name).stem
    file_name = f"{cog_folder}_cog_{options.profile}_ts_{options.tiling_scheme}_q{options.quality}.tif"

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
            quality=options.quality,
            skip_recreate=not options.force_recreate,
            tiling_scheme=options.tiling_scheme,
        )
        logger.info(
            f"COG profile returned for dataset {dataset.id}: {info}",
            extra={"token": token, "dataset_id": dataset.id, "user_id": task.user_id},
        )
    except Exception as e:
        msg = f"Error processing COG for dataset {dataset.id}: {str(e)}"

        # set the status
        update_status(token, dataset.id, StatusEnum.errored)

        # log the error to the database
        logger.error(
            msg,
            extra={"token": token, "user_id": task.user_id, "dataset_id": dataset.id},
        )
        return

    # get the size of the output file
    pass
    # stop the timer
    t2 = time.time()

    # calcute number of overviews
    overviews = len(info.IFD) - 1  # since first IFD is the main image

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
        overviews=overviews,
        tiling_scheme=options.tiling_scheme,
        # !! This is not correct!!
        resolution=int(options.resolution * 100),
        blocksize=info.IFD[0].Blocksize[0],
    )

    # dev
    cog = Cog(**meta)

    with use_client(token) as client:
        try:
            # filter out the None data
            send_data = {k: v for k, v in cog.model_dump().items() if v is not None}
            response = client.table(settings.cogs_table).upsert(send_data).execute()
        except Exception as e:
            msg = f"An error occured while trying to save the COG metadata for dataset {dataset.id}: {str(e)}"

            logger.error(
                msg,
                extra={
                    "token": token,
                    "user_id": task.user_id,
                    "dataset_id": dataset.id,
                },
            )
            update_status(token, dataset.id, StatusEnum.errored)

    # if there was no error, update the status
    update_status(token, dataset.id, StatusEnum.processed)

    # monitoring
    monitoring.cog_counter.inc()
    monitoring.cog_time.observe(cog.runtime)
    monitoring.cog_size.observe(cog.cog_size)

    logger.info(
        f"Finished creating new COG <profile: {cog.compression}> for dataset {dataset.id}.",
        extra={"token": token, "dataset_id": dataset.id, "user_id": task.user_id},
    )
