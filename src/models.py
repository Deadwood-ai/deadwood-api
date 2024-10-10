from typing import Optional
from enum import Enum
from datetime import datetime

from pydantic import BaseModel, field_serializer, field_validator
from pydantic_geojson import MultiPolygonModel
from pydantic_partial import PartialModelMixin
from pydantic_settings import BaseSettings
from rasterio.coords import BoundingBox

from .supabase import SupabaseReader
from .settings import settings


class PlatformEnum(str, Enum):
	drone = 'drone'
	airborne = 'airborne'
	satellite = 'satellite'


class LicenseEnum(str, Enum):
	cc_by = 'CC BY'
	cc_by_sa = 'CC BY-SA'
	cc_by_nc_sa = 'CC BY-NC-SA'
	mit = 'MIT'


class StatusEnum(str, Enum):
	pending = 'pending'
	processing = 'processing'
	errored = 'errored'
	cog_processing = 'cog_processing'
	cog_errored = 'errored'
	thumbnail_processing = 'thumbnail_processing'
	thumbnail_errored = 'thumbnail_errored'
	processed = 'processed'
	audited = 'audited'
	audit_failed = 'audit_failed'


class LabelSourceEnum(str, Enum):
	visual_interpretation = 'visual_interpretation'
	model_prediction = 'model_prediction'
	fixed_model_prediction = 'fixed_model_prediction'


class LabelTypeEnum(str, Enum):
	point_observation = 'point_observation'
	segmentation = 'segmentation'
	instance_segmentation = 'instance_segmentation'
	semantic_segmentation = 'semantic_segmentation'


class ProcessOptions(BaseSettings):
	# overviews: Optional[int] = 8
	resolution: Optional[float] = 0.04
	profile: Optional[str] = 'jpeg'
	quality: Optional[int] = 75
	force_recreate: Optional[bool] = False
	tiling_scheme: Optional[str] = 'web-optimized'


class TaskTypeEnum(str, Enum):
	cog = 'cog'
	thumbnail = 'thumbnail'
	all = 'all'


class TaskPayload(BaseModel):
	id: Optional[int] = None
	dataset_id: int
	user_id: str
	priority: int = 2
	build_args: ProcessOptions = ProcessOptions()
	is_processing: bool = False
	created_at: Optional[datetime] = None
	task_type: TaskTypeEnum


class QueueTask(BaseModel):
	id: int
	dataset_id: int
	user_id: str
	build_args: ProcessOptions
	priority: int
	is_processing: bool
	current_position: int
	estimated_time: float | None = None
	task_type: TaskTypeEnum  # 'cog', 'thumbnail', or 'all'


class Thumbnail(BaseModel):
	dataset_id: int
	thumbnail_path: str
	user_id: str


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
			right, top = ur.strip().split(' ')
			return BoundingBox(
				left=float(left),
				bottom=float(bottom),
				right=float(right),
				top=float(top),
			)
		else:
			return raw_string

	@field_serializer('bbox', mode='plain')
	def bbox_to_postgis(self, bbox: BoundingBox) -> str:
		if bbox is None:
			return None
		return f'BOX({bbox.left} {bbox.bottom}, {bbox.right} {bbox.top})'

	@property
	def centroid(self):
		return (self.bbox.left + self.bbox.right) / 2, (self.bbox.bottom + self.bbox.top) / 2

	@classmethod
	def by_id(cls, id: int, token: str | None = None) -> 'Dataset':
		# instatiate a reader
		reader = SupabaseReader(Model=cls, table=settings.datasets_table, token=token)

		return reader.by_id(id)


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
	project_id: Optional[str] = None
	authors: Optional[str] = None
	spectral_properties: Optional[str] = None
	citation_doi: Optional[str] = None
	additional_information: Optional[str] = None

	# OSM admin levels
	admin_level_1: Optional[str] = None
	admin_level_2: Optional[str] = None
	admin_level_3: Optional[str] = None

	aquisition_year: Optional[int] = None
	aquisition_month: Optional[int] = None
	aquisition_day: Optional[int] = None


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
	# only the aquisition_year is necessary
	aquisition_year: int

	@classmethod
	def by_id(cls, dataset_id: int, token: str | None = None) -> 'Metadata':
		# instatiate a reader
		reader = SupabaseReader(Model=cls, table=settings.metadata_table, token=token)

		return reader.by_id(dataset_id)


class LabelPayloadData(PartialModelMixin, BaseModel):
	"""
	The LabelPayloadData class is the base class for the payload of the label.
	This is the user provided data, before the Labels are validated and saved to
	the database.

	"""

	aoi: MultiPolygonModel
	label: Optional[MultiPolygonModel]
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

	@classmethod
	def by_id(cls, dataset_id: int, token: str | None = None) -> 'Label':
		# instatiate a reader
		reader = SupabaseReader(Model=cls, table=settings.labels_table, token=token)

		return reader.by_id(dataset_id)
