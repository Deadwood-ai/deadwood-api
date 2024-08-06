from enum import Enum
import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
import pandas as pd

from ..models import Dataset, Label, Metadata
from ..settings import settings
from ..deadwood.downloads import bundle_dataset, label_to_geopackage


# create the router for download
router = APIRouter()


# add the format model
class MetadataFormat(str, Enum):
    json = "json"
    csv = "csv"


# main download route
@router.get("/datasets/{dataset_id}")
@router.get("/datasets/{dataset_id}/dataset.zip")
async def download_dataset(dataset_id: str, background_tasks: BackgroundTasks):
    """
    Download the full dataset with the given ID.
    This will create a ZIP file contianing the archived
    original GeoTiff, along with a JSON of the metadata and
    (for dev purpose now) a json schema of the metadata.
    If available, a GeoPackage with the labels will be added.

    """
    # load the dataset
    dataset = Dataset.by_id(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset <ID={dataset_id}> not found.")

    # load the metadata
    metadata = Metadata.by_id(dataset_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail=f"Dataset <ID={dataset_id}> has no associated Metadata entry.")
    
    # load the label
    # TODO: this loads immer nur das erste Label!!!
    label = Label.by_id(dataset_id)

    # build the file name
    archive_file_name = (settings.archive_path / dataset.file_name).resolve()

    # build a temporary zip location
    # TODO: we can use a caching here
    target = tempfile.NamedTemporaryFile(suffix=".zip", delete_on_close=False)
    bundle_dataset(target.name, archive_file_name, metadata=metadata, label=label)

    # remove the temporary file as a background_task
    background_tasks.add_task(lambda: Path(target.name).unlink())

    # now stream the file to the user
    return FileResponse(target.name, media_type='application/zip', filename=f"{Path(metadata.name).stem}.zip")


@router.get("/datasets/{dataset_id}/ortho.tif")
async def download_geotiff(dataset_id: str):
    """
    Download the original GeoTiff of the dataset with the given ID.
    """
        # load the dataset
    dataset = Dataset.by_id(dataset_id)

    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset <ID={dataset_id}> not found.")

    # build the file name
    path = settings.archive_path / dataset.file_name

    return FileResponse(path, media_type='image/tiff', filename=dataset.file_name)


@router.get("/datasets/{dataset_id}/metadata.{file_format}")
async def get_metadata(dataset_id: str, file_format: MetadataFormat, background_tasks: BackgroundTasks):
    """
    Download the metadata of the dataset with the given ID.
    """
    # load the metadata
    metadata = Metadata.by_id(dataset_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail=f"Dataset <ID={dataset_id}> has no Metadata entry.")
    
    # switch the format
    if file_format == MetadataFormat.json:
        return metadata.model_dump_json()
    elif file_format == MetadataFormat.csv:
        # build a DataFrame
        df = pd.DataFrame.from_records([metadata.model_dump()])

        # create a temporary file
        target = tempfile.NamedTemporaryFile(suffix=".csv", delete_on_close=False)
        df.to_csv(target.name, index=False)
        
        # add a background task to remove the file after download
        background_tasks.add_task(lambda: Path(target.name).unlink())

        return FileResponse(target.name, media_type='text/csv', filename="metadata.csv")
    else:
        raise HTTPException(status_code=400, detail=f"Format <{file_format}> not supported.")


@router.get("/datasets/{dataset_id}/labels.gpkg")
async def get_labels(dataset_id: str, background_tasks: BackgroundTasks):
    """
    Download the labels of the dataset with the given ID.
    """
    # load the labels
    label = Label.by_id(dataset_id=dataset_id)
    if label is None:
        raise HTTPException(status_code=404, detail=f"Dataset <ID={dataset_id}> has no labels.")
    
    # create a temporary file
    target = tempfile.NamedTemporaryFile(suffix=".gpkg", delete_on_close=False)
    
    # remove the file after download
    background_tasks.add_task(lambda: Path(target.name).unlink())

    # write the labels
    label_to_geopackage(target.name, label)

    # return the file
    return FileResponse(target.name, media_type='application/geopackage+sqlite', filename="labels.gpkg")
