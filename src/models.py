from typing import Optional
from enum import Enum
from datetime import datetime

from pydantic import BaseModel, field_serializer, field_validator
from pydantic_geojson import MultiPolygonModel, PolygonModel
from pydantic_partial import PartialModelMixin
from pydantic_settings import BaseSettings
from rasterio.coords import BoundingBox


class PlatformEnum(str, Enum):
    drone = "drone"
    airborne = "airborne"
    satellite = "satellite"


class LicenseEnum(str, Enum):
    cc_by = "cc-by"
    cc_by_sa = "cc-by-sa"


class StatusEnum(str, Enum):
    pending = "pending"
    processing = "processing"
    errored = "errored"
    processed = "processed"
    audited = "audited"
    audit_failed = "audit_failed"


class LabelSourceEnum(str, Enum):
    visual_interpretation = "visual_interpretation"
    model_prediction = "model_prediction"
    fixed_model_prediction = "fixed_model_prediction"


class LabelTypeEnum(str, Enum):
    point_observation = "point_observation"
    segmentation = "segmentation"
    instance_segmentation = "instance_segmentation"
    semantic_segmentation = "semantic_segmentation"


class ProcessOptions(BaseSettings):
    overviews: Optional[int] = 8
    resolution: Optional[float] = 0.04
    profile: Optional[str] = "jpeg"
    quality: Optional[int] = 75
    force_recreate: Optional[bool] = False


class TaskPayload(BaseModel):
    id: Optional[int] = None
    dataset_id: int
    user_id: str
    priority: int = 2
    build_args: ProcessOptions = ProcessOptions()
    is_processing: bool = False
    created_at: Optional[datetime] = None


class QueueTask(TaskPayload):
    estimated_time: float
    current_position: int


class Dataset(BaseModel):
    """
    The Dataset class is the base class for each Dataset object in the database.
    It contains the minimum required metadata to upload a GeoTiff and start processing.
    It also contains the metadata, that cannot be changed after the upload by the user anymore.
    
    Additionally, it will be linked to the Metadata record, which is updatable for the user,
    and links the Labels with a 1:m cardinality.
    """
    id: Optional[int] = None
    file_name: str
    file_alias: str
    file_size: int
    copy_time: float
    sha256: str
    bbox: BoundingBox
    status: StatusEnum
    user_id: str
    created_at: Optional[datetime] = None
    
    @field_serializer('created_at', mode='plain')
    def datetime_to_isoformat(field: datetime | None) -> str | None:
        if field is None:
            return None
        return field.isoformat()
    
    @field_validator('bbox', mode='before')
    @classmethod
    def transform_bbox(cls, raw_string: str | BoundingBox) -> BoundingBox:
        if isinstance(raw_string, str):
            # parse the string
            s = raw_string.replace('BOX(', '').replace(')', '')
            ll, ur = s.split(',')
            left, bottom = ll.strip().split(' ')
            right, upper = ur.strip().split(' ')
            return BoundingBox(left=float(left), bottom=float(bottom), right=float(right), top=float(upper))
        else:
            return raw_string

    @field_serializer('bbox', mode='plain')
    def bbox_to_postgis(self, bbox: BoundingBox) -> str:
        if bbox is None:
            return None
        return f"BOX({bbox.left} {bbox.bottom}, {bbox.right} {bbox.top})"
    
    @property
    def centroid(self):
        return (self.bbox.left + self.bbox.right) / 2, (self.bbox.bottom + self.bbox.top) / 2


class Cog(BaseModel):
    """
    The Cog class is the base class for the cloud optimized geotiff.
    Currently it is modelled using a 1:1 cardinality. It is not in its own table 
    as the user_id is the processor which created the file (the user cannot change
    the properties of the COG, but we can)
    """
    # primary key
    dataset_id: int
    cog_folder: str
    cog_name: str

    # basic metadata
    cog_url: str
    cog_size: int
    runtime: float
    user_id: str

    # COG options
    compression: str
    overviews: int
    resolution: int
    blocksize: Optional[int] = None
    compression_level: Optional[str] = None
    tiling_scheme: Optional[str] = None

    created_at: Optional[datetime] = None

    @field_serializer('created_at', mode='plain')
    def datetime_to_isoformat(field: datetime | None) -> str | None:
        if field is None:
            return None
        return field.isoformat()


class MetadataPayloadData(PartialModelMixin, BaseModel):
    # now the metadata
    name: Optional[str] = None
    license: Optional[LicenseEnum] = None
    platform: Optional[PlatformEnum] = None
    project_id: Optional[int] = None
    authors: Optional[str] = None
    spectral_properties: Optional[str] = None
    citation_doi: Optional[str] = None
    
    # Gadm labels
    gadm_name_1: Optional[str] = None
    gadm_name_2: Optional[str] = None
    gadm_name_3: Optional[str] = None

    aquisition_date: Optional[datetime] = None
    
    @field_serializer('aquisition_date', mode='plain')
    def datetime_to_isoformat(field: datetime | None) -> str | None:
        if field is None:
            return None
        return field.isoformat()


class Metadata(MetadataPayloadData):
    """
    Class for additional Metadata in the database. It has to be connected to a Dataset object
    using a 1:1 cardinality.
    This is separated, so that different RLS policies can apply. Additionally, this is the 
    metadata that can potentially be 
    """
    # primary key
    dataset_id: int
    
    # link to a user
    user_id: str

    # make some field non-optional
    name: str
    license: LicenseEnum
    platform: PlatformEnum
    aquisition_date: datetime


class LabelPayloadData(PartialModelMixin, BaseModel):
    """
    The LabelPayloadData class is the base class for the payload of the label.
    This is the user provided data, before the Labels are validated and saved to
    the database.

    """
    aoi: PolygonModel
    label: MultiPolygonModel
    label_source: LabelSourceEnum
    label_quality: int
    label_type: LabelTypeEnum

PartialLabelPayloadData = LabelPayloadData.model_as_partial()


class Label(LabelPayloadData):
    """
    The Label class represents one set of a label - aoi combination.
    Both need to be a single MULTIPOLYGON.
    """
    # primary key
    id: Optional[int] = None
    
    # the label
    dataset_id: int
    user_id: str

    created_at: Optional[datetime] = None
    
    @field_serializer('created_at', mode='plain')
    def datetime_to_isoformat(field: datetime | None) -> str | None:
        if field is None:
            return None
        return field.isoformat()
