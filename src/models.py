from typing import Optional
from enum import Enum
from datetime import datetime

from pydantic import BaseModel, field_serializer, computed_field, field_validator
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


class FileUploadMetadata(BaseModel):
    user_id: str
    aquisition_date: datetime
    upload_date: datetime
    file_name: str
    content_type: str
    file_size: int
    raw_path: str
    processed_path: Optional[str] = None
    copy_time: float
    uuid: str
    sha256: str
    platform: PlatformEnum
    license: LicenseEnum
    status: StatusEnum = StatusEnum.pending

    # optional fields
    compress_time: Optional[float] = None
    wms_source: Optional[str] = None
    bbox: Optional[BoundingBox] = None


    @computed_field
    @property
    def file_id(self) -> str:
        return f"{self.uuid}_{self.file_name}"

    @field_serializer('aquisition_date', 'upload_date', mode='plain')
    def datetime_to_isoformat(field: datetime) -> str:
        return field.isoformat()
    
    @field_validator('bbox', mode='before')
    @classmethod
    def transform_bbox(cls, raw_string: str | BoundingBox) -> BoundingBox:
        if isinstance(raw_string, str):
            # parse the string
            s = raw_string.replace('BOX(', '').replace(')', '')
            ll, ur = s.split(',')
            left, bottom = ll.split(' ')
            right, upper = ur.split(' ')
            return BoundingBox(float(left), float(bottom), float(right), float(upper))
        else:
            return raw_string

    @field_serializer('bbox', mode='plain')
    def bbox_to_postgis(field: BoundingBox) -> str:
        if field is None:
            return None
        return f"BOX({field.bottom} {field.left}, {field.top} {field.right})"