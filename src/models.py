from typing import Optional
from enum import Enum
from datetime import datetime

from pydantic import BaseModel, field_serializer, field_validator
from rasterio.coords import BoundingBox


class PlatformEnum(str, Enum):
    drone = "drone"
    airborne = "airborne"
    sattelite = "sattelite"


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


class Dataset(BaseModel):
    """
    The Dataset class is the base class for each Dataset object in the database.
    It contains the minimum required metadata to upload a GeoTiff and start processing.
    It also contains the metadata, that cannot be changed after the upload by the user anymore.
    
    Additionally, it will be linked to the Metacata record, which is updatable for the user,
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


class Cog(BaseModel):
    """
    The Cog class is the base class for the cloud optimized geotiff.
    Currently it is modelled using a 1:1 cardinality. It is not in its own table 
    as the user_id is the processor which created the file (the user cannot change
    the properties of the COG, but we can)
    """
    # primary key
    dataset_id: str
    cog_folder: str
    cog_name: str

    # basic metadata
    cog_url: str
    cog_size: int
    runtime: float
    user_id: str

    # COG options
    compression: str
    overviews: str
    resolution: int
    blocksize: Optional[int] = None
    compression_level: Optional[str] = None
    tiling_scheme: Optional[str] = None

    created_at: datetime

    @field_serializer('created_at', mode='plain')
    def datetime_to_isoformat(field: datetime) -> str:
        return field.isoformat()


class Metadata(BaseModel):
    """
    Class for additional Metadata in the database. It has to be connected to a Dataset object
    using a 1:1 cardinality.
    This is separated, so that different RLS policies can apply. Additionally, this is the 
    metadata that can potentially be 
    """
    # primary key
    dataset_id: str
    
    user_id: str

    # now the metadata
    name: str
    license: LicenseEnum
    platform: PlatformEnum
    project_id: Optional[int] = None
    authors: Optional[str] = None
    spectral_properties: Optional[str] = None
    citation_doi: Optional[str] = None
    
    # Gadm labels
    gadm_name_1: Optional[str] = None
    gadm_name_2: Optional[str] = None
    gadm_name_3: Optional[str] = None

    aquisition_date: datetime
    
    @field_serializer('aquisition_date', mode='plain')
    def datetime_to_isoformat(field: datetime) -> str:
        return field.isoformat()


class Label(BaseModel):
    """
    The Label class represents one set of a label - aoi combination.
    Both need to be a single MULTIPOLYGON.
    """
    # primary key
    id: int
    
    # the label
    dataset_id: str
    user_id: str
    aoi: dict
    label: dict
    label_source: str
    label_quality: str

    created_at: datetime
    
    @field_serializer('created_at', mode='plain')
    def datetime_to_isoformat(field: datetime) -> str:
        return field.isoformat()
