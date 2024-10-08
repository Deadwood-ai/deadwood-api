from pathlib import Path
import time
import paramiko
import os
import base64
import tempfile
import shutil

from fastapi import HTTPException

from .supabase import use_client, login, verify_token
from .settings import settings
from .models import StatusEnum, Dataset, QueueTask, Cog, Thumbnail
from .logger import logger
from . import monitoring
from .deadwood.cog import calculate_cog
from .deadwood.thumbnail import calculate_thumbnail


def pull_file_from_storage_server(remote_file_path: str, local_file_path: str):
    # Check if the file already exists locally
    if os.path.exists(local_file_path):
        print(f"File already exists locally at: {local_file_path}")
        return

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(
        "connecting to:",
        settings.storage_server_ip,
        settings.storage_server_username,
        settings.storage_server_password,
    )
    ssh.connect(
        hostname=settings.storage_server_ip,
        username=settings.storage_server_username,
        password=settings.storage_server_password,
        port=22,  # Add this line to specify the default SSH port
    )

    sftp = ssh.open_sftp()
    print("pulling file: ", remote_file_path, "to", local_file_path)

    # Create the directory for local_file_path if it doesn't exist
    local_dir = Path(local_file_path).parent
    local_dir.mkdir(parents=True, exist_ok=True)

    sftp.get(remote_file_path, local_file_path)
    print("file pulled")
    sftp.close()
    ssh.close()

    # Check if the file exists after pulling
    if os.path.exists(local_file_path):
        print(f"File successfully saved at: {local_file_path}")
        print(f"File size: {os.path.getsize(local_file_path)} bytes")
    else:
        print(f"Error: File not found at {local_file_path} after pulling")


def push_file_to_storage_server(local_file_path: str, remote_file_path: str):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=settings.storage_server_ip,
        username=settings.storage_server_username,
        password=settings.storage_server_password,
        port=22,  # Add this line to specify the default SSH port
    )
    sftp = ssh.open_sftp()
    # Extract the remote directory path
    remote_dir = os.path.dirname(remote_file_path)

    try:
        # Create the remote directory if it doesn't exist
        sftp.mkdir(remote_dir)
    except IOError:
        # Directory might already exist, which is fine
        pass
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


def process_cog(task: QueueTask, temp_dir: Path):
    # login with the processor
    token = login(
        settings.processor_username, settings.processor_password
    ).session.access_token

    user = verify_token(token)
    print("user is:", user)
    if not user:
        return HTTPException(status_code=401, detail="Invalid token")

    with use_client(token) as client:
        response = (
            client.table(settings.datasets_table)
            .select("*")
            .eq("id", task.dataset_id)
            .execute()
        )
        dataset = Dataset(**response.data[0])

    # update the status to processing
    update_status(token, dataset_id=dataset.id, status=StatusEnum.cog_processing)
    print("running cog")

    # get local file path
    input_path = Path(temp_dir) / dataset.file_name

    # get the remote file path
    storage_server_file_path = (
        f"{settings.storage_server_data_path}/archive/{dataset.file_name}"
    )

    # pull the file from the storage server
    logger.info(
        f"Pulling tiff from storage server: {storage_server_file_path} to {input_path}",
        extra={"token": token, "dataset_id": dataset.id, "user_id": user.id},
    )
    pull_file_from_storage_server(storage_server_file_path, str(input_path))

    # get the options
    options = task.build_args

    # get the output settings
    cog_folder = Path(dataset.file_name).stem
    file_name = f"{cog_folder}_cog_{options.profile}_ts_{options.tiling_scheme}_q{options.quality}.tif"

    # output path is in the temporary directory
    output_path = Path(temp_dir) / file_name

    t1 = time.time()
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
        extra={"token": token, "dataset_id": dataset.id, "user_id": user.id},
    )

    # push the file to the storage server
    storage_server_cog_path = (
        f"{settings.storage_server_data_path}/cogs/{cog_folder}/{file_name}"
    )
    logger.info(
        f"Pushing cog to storage server: {output_path} to {storage_server_cog_path}",
        extra={"token": token, "dataset_id": dataset.id, "user_id": user.id},
    )
    push_file_to_storage_server(str(output_path), storage_server_cog_path)

    t2 = time.time()

    # calculate number of overviews
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
        resolution=int(options.resolution * 100),
        blocksize=info.IFD[0].Blocksize[0],
    )

    # save the metadata to the database
    cog = Cog(**meta)

    with use_client(token) as client:
        send_data = {k: v for k, v in cog.model_dump().items() if v is not None}
        client.table(settings.cogs_table).upsert(send_data).execute()

    # If we reach here, processing was successful
    update_status(token, dataset.id, StatusEnum.processed)

    # monitoring
    monitoring.cog_counter.inc()
    monitoring.cog_time.observe(cog.runtime)
    monitoring.cog_size.observe(cog.cog_size)

    logger.info(
        f"Finished creating new COG <profile: {cog.compression}> for dataset {dataset.id}.",
        extra={"token": token, "dataset_id": dataset.id, "user_id": user.id},
    )


def process_thumbnail(task: QueueTask, temp_dir: Path):
    # login with the processor
    token = login(
        settings.processor_username, settings.processor_password
    ).session.access_token

    user = verify_token(token)
    print("user is:", user)
    if not user:
        return HTTPException(status_code=401, detail="Invalid token")

    # load the dataset
    with use_client(token) as client:
        response = (
            client.table(settings.datasets_table)
            .select("*")
            .eq("id", task.dataset_id)
            .execute()
        )
        dataset = Dataset(**response.data[0])

    # update the status to processing
    update_status(token, dataset_id=dataset.id, status=StatusEnum.processing)

    # get local file paths
    input_path = Path(temp_dir) / dataset.file_name
    thumbnail_file_name = dataset.file_name.replace(".tif", ".jpg")
    output_path = Path(temp_dir) / thumbnail_file_name

    # get the remote file path
    storage_server_file_path = (
        f"{settings.storage_server_data_path}/archive/{dataset.file_name}"
    )

    # pull the file from the storage server
    logger.info(
        f"Pulling tiff from storage server: {storage_server_file_path} to {input_path}",
        extra={"token": token, "dataset_id": dataset.id, "user_id": user.id},
    )
    pull_file_from_storage_server(storage_server_file_path, str(input_path))

    t1 = time.time()
    calculate_thumbnail(str(input_path), str(output_path))
    logger.info(
        f"Thumbnail generated for dataset {dataset.id}",
        extra={"token": token, "dataset_id": dataset.id, "user_id": user.id},
    )

    # push the file to the storage server
    storage_server_thumbnail_path = (
        f"{settings.storage_server_data_path}/thumbnails/{thumbnail_file_name}"
    )
    logger.info(
        f"Pushing thumbnail to storage server: {output_path} to {storage_server_thumbnail_path}",
        extra={"token": token, "dataset_id": dataset.id, "user_id": user.id},
    )
    push_file_to_storage_server(str(output_path), storage_server_thumbnail_path)

    t2 = time.time()

    # fill the metadata
    meta = dict(
        dataset_id=dataset.id,
        user_id=task.user_id,
        thumbnail_path=thumbnail_file_name,
    )

    thumbnail = Thumbnail(**meta)

    # save the metadata to the database
    with use_client(token) as client:
        # check if thumbnail already exists, delete it
        response = (
            client.table(settings.thumbnail_table)
            .select("*")
            .eq("dataset_id", dataset.id)
            .execute()
        )
        if len(response.data) > 0:
            client.table(settings.thumbnail_table).delete().eq(
                "dataset_id", dataset.id
            ).execute()

        # insert new thumbnail data
        client.table(settings.thumbnail_table).insert(thumbnail.model_dump()).execute()

    # If we reach here, processing was successful
    update_status(token, dataset.id, StatusEnum.processed)

    logger.info(
        f"Finished creating thumbnail for dataset {dataset.id}.",
        extra={"token": token, "dataset_id": dataset.id, "user_id": user.id},
    )
