from enum import Enum

from fastapi import APIRouter
from fastapi.responses import FileResponse


# create the router for download
router = APIRouter()


# add the format model
class MetadataFormat(str, Enum):
    json = "json"
    csv = "csv"


# main download route
@router.get("/datasets/{dataset_id}")
def download_dataset(dataset_id: str):
    """
    Download the full dataset with the given ID.
    This will create a ZIP file contianing the archived
    original GeoTiff, along with a JSON of the metadata and
    (for dev purpose now) a json schema of the metadata.
    If available, a GeoPackage with the labels will be added.

    """
    # load the dataset info from the database
    raise NotImplementedError


@router.get("/datasets/{dataset_id}/image.tif")
def download_geotiff(dataset_id: str):
    """
    Download the original GeoTiff of the dataset with the given ID.
    """
    raise NotImplementedError


@router.get("/datasets/{dataset_id}/metadata.{file_format}")
def get_metadata(dataset_id: str, file_format: MetadataFormat):
    """
    Download the metadata of the dataset with the given ID.
    """
    raise NotImplementedError


@router.get("/datasets/{dataset_id}/labels.gpkg")
def get_labels(dataset_id: str):
    """
    Download the labels of the dataset with the given ID.
    """
    raise NotImplementedError
