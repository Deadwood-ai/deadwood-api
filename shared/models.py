from typing import Optional, List, Dict, Any, Tuple, Literal, Union, Annotated
from enum import Enum
from datetime import datetime

from pydantic import AliasChoices, BaseModel, field_serializer, field_validator, model_validator, Field
from pydantic_partial import PartialModelMixin
from pydantic_settings import BaseSettings
from rasterio.coords import BoundingBox

from .asset_manifest import (
	AOI_V1_MODEL_CHECKPOINT_NAME,
	COMBINED_MODEL_CHECKPOINT_NAME,
	DEADWOOD_V1_MODEL_CHECKPOINT_NAME,
)
from .settings import settings


# Custom 2D-only GeoJSON models (replacing pydantic_geojson which adds alt=None)
# This prevents float(None) errors when converting to shapely geometries

Coordinate2D = Tuple[
	Annotated[Union[float, int], Field(ge=-180, le=180)],  # lon
	Annotated[Union[float, int], Field(ge=-90, le=90)],    # lat
]


class PolygonModel(BaseModel):
	"""2D-only Polygon GeoJSON model."""
	type: Literal["Polygon"] = "Polygon"
	coordinates: List[List[Coordinate2D]]


class MultiPolygonModel(BaseModel):
	"""2D-only MultiPolygon GeoJSON model."""
	type: Literal["MultiPolygon"] = "MultiPolygon"
	coordinates: List[List[List[Coordinate2D]]]


class LabelDataEnum(str, Enum):
	deadwood = 'deadwood'
	forest_cover = 'forest_cover'


COMBINED_MODEL_MODULE = 'deadwood_treecover_combined_v2'
COMBINED_MODEL_CONFIG = {
	'module': COMBINED_MODEL_MODULE,
	'checkpoint_name': COMBINED_MODEL_CHECKPOINT_NAME,
}

DEADWOOD_V1_MODEL_CONFIG = {
	'module': 'deadwood_segmentation_v1_moehring',
	'checkpoint_name': DEADWOOD_V1_MODEL_CHECKPOINT_NAME,
}

TREECOVER_V1_MODEL_CONFIG = {
	'module': 'treecover_segmentation_oam_tcd',
	'checkpoint_name': 'restor/tcd-segformer-mit-b5',
}

# Automatic Area-of-Interest (AOI) segmentation model. Produces one coarse AOI
# polygon per orthomosaic that an auditor reviews/edits before the dataset is
# audited. SegFormer-B1, binary inside/outside-AOI classification.
AOI_V1_MODEL_MODULE = 'aoi_segmentation_v1'
AOI_V1_MODEL_CONFIG = {
	'module': AOI_V1_MODEL_MODULE,
	'checkpoint_name': AOI_V1_MODEL_CHECKPOINT_NAME,
}


DEFAULT_MODEL_PREFERENCES = {
	LabelDataEnum.deadwood: dict(DEADWOOD_V1_MODEL_CONFIG),
	LabelDataEnum.forest_cover: dict(TREECOVER_V1_MODEL_CONFIG),
}


class PlatformEnum(str, Enum):
	drone = 'drone'
	airborne = 'airborne'
	satellite = 'satellite'


class LicenseEnum(str, Enum):
	cc_by = 'CC BY'
	cc_by_sa = 'CC BY-SA'
	cc_by_nc_sa = 'CC BY-NC-SA'
	mit = 'MIT'
	cc_by_nc = 'CC BY-NC'


class StatusEnum(str, Enum):
	idle = 'idle'
	uploading = 'uploading'
	ortho_processing = 'ortho_processing'
	cog_processing = 'cog_processing'
	metadata_processing = 'metadata_processing'
	odm_processing = 'odm_processing'
	thumbnail_processing = 'thumbnail_processing'
	deadwood_segmentation = 'deadwood_segmentation'
	forest_cover_segmentation = 'forest_cover_segmentation'
	deadwood_treecover_combined_segmentation = 'deadwood_treecover_combined_segmentation'
	aoi_segmentation = 'aoi_segmentation'
	embedding_processing = 'embedding_processing'
	audit_in_progress = 'audit_in_progress'


class DatasetAccessEnum(str, Enum):
	public = 'public'
	private = 'private'
	viewonly = 'viewonly'


class LabelSourceEnum(str, Enum):
	visual_interpretation = 'visual_interpretation'
	model_prediction = 'model_prediction'
	fixed_model_prediction = 'fixed_model_prediction'
	reference_patch = 'reference_patch'


class LabelTypeEnum(str, Enum):
	point_observation = 'point_observation'
	segmentation = 'segmentation'
	instance_segmentation = 'instance_segmentation'
	semantic_segmentation = 'semantic_segmentation'


class PredictionQualityEnum(str, Enum):
	great = 'great'
	sentinel_ok = 'sentinel_ok'
	bad = 'bad'


class TaskTypeEnum(str, Enum):
	cog = 'cog'  # Generate cloud optimized geotiff
	thumbnail = 'thumbnail'  # Generate thumbnail image
	deadwood_v1 = 'deadwood_v1'  # Run deadwood segmentation
	treecover_v1 = 'treecover_v1'  # Run tree cover segmentation
	deadwood_treecover_combined_v2 = 'deadwood_treecover_combined_v2'  # Run combined deadwood+treecover segmentation
	aoi_v1 = 'aoi_v1'  # Auto-generate the dataset area of interest (AOI) polygon
	embeddings_v1 = 'embeddings_v1'  # Compute per-tile CLIP embeddings for open-vocab search
	geotiff = 'geotiff'  # Convert to geotiff
	metadata = 'metadata'  # Extract metadata
	odm_processing = 'odm_processing'  # ODM raw image processing

	@classmethod
	def _missing_(cls, value):
		legacy_aliases = {
			'deadwood': cls.deadwood_v1,
			'treecover': cls.treecover_v1,
		}
		if isinstance(value, str):
			return legacy_aliases.get(value)
		return None

	@property
	def display_name(self) -> str:
		"""Human-readable display name for the task type."""
		display_names = {
			'cog': 'COG',
			'thumbnail': 'Thumbnail',
			'deadwood_v1': 'Deadwood',
			'treecover_v1': 'Tree Cover',
			'deadwood_treecover_combined_v2': 'Deadwood+Treecover (v2)',
			'aoi_v1': 'AOI',
			'embeddings_v1': 'Embeddings',
			'geotiff': 'GeoTIFF',
			'metadata': 'Metadata',
			'odm_processing': 'ODM',
		}
		return display_names.get(self.value, self.value)

	@classmethod
	def from_string(cls, value: str) -> 'TaskTypeEnum | None':
		"""Get TaskTypeEnum from string value, including legacy aliases, returns None if not found."""
		try:
			return cls(value)
		except ValueError:
			return None


class TaskPayload(BaseModel):
	id: Optional[int] = None
	dataset_id: int
	user_id: str
	priority: int = 2
	is_processing: bool = False
	claimed_by: Optional[str] = None
	claimed_at: Optional[datetime] = None
	created_at: Optional[datetime] = None
	task_types: List[TaskTypeEnum]


class QueueTask(BaseModel):
	id: int
	dataset_id: int
	user_id: str
	priority: int
	is_processing: bool
	claimed_by: Optional[str] = None
	claimed_at: Optional[datetime] = None
	current_position: int
	estimated_time: float | None = None
	task_types: List[TaskTypeEnum]


class Status(BaseModel):
	"""
	Tracks the processing status and completion states for a dataset
	"""

	id: Optional[int] = None
	dataset_id: int
	current_status: StatusEnum = StatusEnum.idle
	is_upload_done: bool = False
	is_ortho_done: bool = False
	is_cog_done: bool = False
	is_thumbnail_done: bool = False
	is_deadwood_done: bool = False
	is_forest_cover_done: bool = False
	is_combined_model_done: bool = False
	is_aoi_done: bool = False
	is_aoi_required: bool = False
	is_embeddings_done: bool = False
	is_metadata_done: bool = False
	is_odm_done: bool = False
	has_error: bool = False
	error_message: Optional[str] = None
	error_stage: Optional[str] = None
	created_at: Optional[datetime] = None
	updated_at: Optional[datetime] = None

	@field_serializer('created_at', 'updated_at', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()


class Thumbnail(BaseModel):
	dataset_id: int
	thumbnail_path: str
	thumbnail_file_name: str
	thumbnail_file_size: int
	version: int
	thumbnail_processing_runtime: float


class Dataset(PartialModelMixin, BaseModel):
	"""
	V2Dataset combines the previous Dataset and Metadata models into a single model
	with only user-provided information that doesn't change after creation.
	"""

	id: Optional[int] = None
	user_id: str
	created_at: Optional[datetime] = None
	file_name: str
	license: LicenseEnum
	platform: PlatformEnum
	project_id: Optional[str] = None
	authors: List[str]
	aquisition_year: Optional[int] = None
	aquisition_month: Optional[int] = None
	aquisition_day: Optional[int] = None
	additional_information: Optional[str] = None
	data_access: DatasetAccessEnum = DatasetAccessEnum.public
	citation_doi: Optional[str] = None
	archived: bool = False

	@field_serializer('created_at', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()

	@field_validator('aquisition_year')
	@classmethod
	def validate_year(cls, v: Optional[int]) -> Optional[int]:
		if v is not None and (v < 1980 or v > 2099):
			raise ValueError('Year must be between 1980 and 2099')
		return v

	@field_validator('aquisition_month')
	@classmethod
	def validate_month(cls, v: Optional[int]) -> Optional[int]:
		if v is not None and (v < 1 or v > 12):
			raise ValueError('Month must be between 1 and 12')
		return v

	@field_validator('aquisition_day')
	@classmethod
	def validate_day(cls, v: Optional[int]) -> Optional[int]:
		if v is not None and (v < 1 or v > 31):
			raise ValueError('Day must be between 1 and 31')
		return v


class RawImages(BaseModel):
	"""
	Raw drone images metadata and RTK data information.

	This model stores metadata about uploaded raw drone image collections,
	including comprehensive camera EXIF metadata extracted from the images.
	"""

	id: Optional[int] = None
	dataset_id: int
	raw_image_count: int
	raw_image_size_mb: int
	raw_images_path: str  # Contains both images and RTK files
	camera_metadata: Optional[Dict[str, Any]] = None
	"""
	Flexible EXIF metadata extracted from drone images stored as JSONB.
	
	Structure varies by camera manufacturer but typically includes:
	
	**Camera Information:**
	- Make, Model, Software, Serial numbers
	- Examples: DJI: "Make": "DJI", "Model": "L2"
	          Canon: "Make": "Canon", "Model": "EOS R5"
	          
	**Image Settings:**
	- ISO, aperture, shutter speed, focal length
	- Examples: "ISOSpeedRatings": 100, "FNumber": 2.8, "FocalLength": 12.29
	
	**Acquisition Details:**
	- Timestamps, GPS coordinates, altitude
	- Examples: "DateTime": "2025:04:03 12:53:33"
	          "GPSLatitude": [lat, lat_ref], "GPSLongitude": [lon, lon_ref]
	
	**Technical Specifications:**
	- Image dimensions, color space, compression
	- Examples: "ExifImageWidth": 5280, "ExifImageHeight": 3956
	
	**Note:** Field names and structures vary significantly between manufacturers.
	This flexible approach accommodates DJI, Canon, Nikon, Sony, Phantom, 
	and other drone/camera systems without imposing rigid schema constraints.
	
	The extraction process automatically handles manufacturer differences,
	sanitizes problematic characters, and filters out non-serializable data.
	"""
	has_rtk_data: bool = False
	rtk_precision_cm: Optional[float] = None
	rtk_quality_indicator: Optional[int] = None
	rtk_file_count: int = 0
	version: int = 1
	created_at: Optional[datetime] = None

	@field_serializer('created_at', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()


class Cog(BaseModel):
	"""
	Represents the cloud optimized geotiff processing results
	"""

	dataset_id: int
	cog_file_size: int
	cog_file_name: str
	cog_path: str
	version: int
	created_at: Optional[datetime] = None
	cog_info: Optional[Dict] = None
	cog_processing_runtime: Optional[float] = None

	@field_serializer('created_at', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()


class Ortho(BaseModel):
	"""
	Represents the original orthophoto file information
	"""

	# id: Optional[int] = None
	dataset_id: int
	ortho_file_name: str
	ortho_file_size: int
	version: int
	created_at: Optional[datetime] = None
	bbox: Optional[BoundingBox] = None
	sha256: Optional[str] = None
	ortho_info: Optional[Dict] = None
	ortho_upload_runtime: Optional[float] = None

	@field_serializer('created_at', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()

	@field_validator('bbox', mode='before')
	@classmethod
	def transform_bbox(cls, raw_string: Optional[str | BoundingBox]) -> Optional[BoundingBox]:
		if raw_string is None:
			return None
		if isinstance(raw_string, str):
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
		return raw_string

	@field_serializer('bbox', mode='plain')
	def bbox_to_postgis(self, bbox: Optional[BoundingBox]) -> Optional[str]:
		if bbox is None:
			return None
		return f'BOX({bbox.left} {bbox.bottom},{bbox.right} {bbox.top})'


class ProcessedOrtho(BaseModel):
	"""
	Represents the processed orthophoto file information
	"""

	# id: Optional[int] = None
	dataset_id: int
	ortho_file_name: str
	ortho_file_size: int
	version: int
	created_at: Optional[datetime] = None
	bbox: Optional[BoundingBox] = None
	sha256: Optional[str] = None
	ortho_info: Optional[Dict] = None
	ortho_processing_runtime: Optional[float] = None

	@field_serializer('created_at', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()

	@field_validator('bbox', mode='before')
	@classmethod
	def transform_bbox(cls, raw_string: Optional[str | BoundingBox]) -> Optional[BoundingBox]:
		if raw_string is None:
			return None
		if isinstance(raw_string, str):
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
		return raw_string

	@field_serializer('bbox', mode='plain')
	def bbox_to_postgis(self, bbox: Optional[BoundingBox]) -> Optional[str]:
		if bbox is None:
			return None
		return f'BOX({bbox.left} {bbox.bottom},{bbox.right} {bbox.top})'


class LabelPayloadData(PartialModelMixin, BaseModel):
	"""
	The LabelPayloadData class is the base class for the payload of the label.
	This is the user provided data, before the Labels are validated and saved to
	the database.
	"""

	# AOI related fields
	aoi_geometry: Optional[MultiPolygonModel] = None
	aoi_is_whole_image: bool = False
	aoi_image_quality: Optional[int] = None
	aoi_notes: Optional[str] = None

	# Label related fields
	dataset_id: int
	label_source: LabelSourceEnum
	label_type: LabelTypeEnum
	label_data: LabelDataEnum
	label_quality: Optional[int] = None
	model_metadata: Optional[Dict[str, Any]] = Field(
		default=None,
		validation_alias=AliasChoices('model_config', 'model_metadata'),
		serialization_alias='model_config',
	)

	# Label geometry
	geometry: MultiPolygonModel
	properties: Optional[Dict[str, Any]] = None

	@field_validator('aoi_image_quality', 'label_quality')
	def validate_quality(cls, v):
		if v is not None and not 1 <= v <= 3:
			raise ValueError('Quality must be between 1 and 3')
		return v


PartialLabelPayloadData = LabelPayloadData.model_as_partial()


class UserLabelObject(BaseModel):
	dataset_id: int
	user_id: str
	file_type: str
	file_alias: str
	file_path: str
	label_description: str
	audited: bool


class AOI(BaseModel):
	"""Area of Interest model for v2_aois table"""

	id: Optional[int] = None
	dataset_id: int
	user_id: str
	geometry: Dict  # Changed from MultiPolygonModel to Dict
	is_whole_image: bool = False
	source: Literal['ml_prediction', 'manual', 'manual_correction'] = 'manual'
	corrected_from_aoi_id: Optional[int] = None
	image_quality: Optional[int] = None
	notes: Optional[str] = None
	created_at: Optional[datetime] = None
	updated_at: Optional[datetime] = None

	@field_validator('image_quality')
	def validate_image_quality(cls, v):
		if v is not None and not 1 <= v <= 3:
			raise ValueError('Image quality must be between 1 and 3')
		return v


def aoi_insert_payload(aoi: AOI) -> Dict:
	"""Serialize AOIs for rolling compatibility with the pre-provenance schema."""
	return aoi.model_dump(exclude={'id', 'created_at', 'updated_at', 'source', 'corrected_from_aoi_id'})


class Label(BaseModel):
	"""Label model for v2_labels table"""

	id: Optional[int] = None
	dataset_id: int
	aoi_id: Optional[int] = None
	user_id: str
	label_source: LabelSourceEnum
	label_type: LabelTypeEnum
	label_data: LabelDataEnum
	label_quality: Optional[int] = None
	model_metadata: Optional[Dict[str, Any]] = Field(
		default=None,
		validation_alias=AliasChoices('model_config', 'model_metadata'),
		serialization_alias='model_config',
	)
	is_active: bool = True
	parent_label_id: Optional[int] = None
	reference_patch_id: Optional[int] = None
	version: int = 1
	created_at: Optional[datetime] = None
	updated_at: Optional[datetime] = None

	@field_validator('label_quality')
	def validate_label_quality(cls, v):
		if v is not None and not 1 <= v <= 3:
			raise ValueError('Label quality must be between 1 and 3')
		return v


class ModelPreference(BaseModel):
	"""Stores the preferred model_config per label_data type (v2_model_preferences table)."""

	id: Optional[int] = None
	label_data: LabelDataEnum
	preferred_model_config: Dict[str, Any] = Field(alias='model_config')
	created_at: Optional[datetime] = None
	updated_at: Optional[datetime] = None

	model_config = {'populate_by_name': True}


class DeadwoodGeometry(BaseModel):
	"""Label geometry model for v2_deadwood_geometries table"""

	id: Optional[int] = None
	label_id: int
	geometry: PolygonModel
	properties: Optional[Dict[str, Any]] = None
	created_at: Optional[datetime] = None

	@field_serializer('created_at', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()


class ForestCoverGeometry(BaseModel):
	"""Label geometry model for v2_forest_cover_geometries table"""

	id: Optional[int] = None
	label_id: int
	geometry: PolygonModel
	properties: Optional[Dict[str, Any]] = None
	created_at: Optional[datetime] = None

	@field_serializer('created_at', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()


class MetadataType(str, Enum):
	GADM = 'gadm'
	BIOME = 'biome'
	PHENOLOGY = 'phenology'
	# Add more types as needed


class AdminBoundariesMetadata(BaseModel):
	"""Structure for GADM administrative boundaries metadata"""

	admin_level_1: Optional[str] = None  # Country
	admin_level_2: Optional[str] = None  # State/Province
	admin_level_3: Optional[str] = None  # District
	source: str = 'GADM'
	version: str = '4.1.0'  # GADM version


class BiomeMetadata(BaseModel):
	"""Structure for WWF Terrestrial Ecoregions biome metadata"""

	biome_name: Optional[str] = None
	biome_id: Optional[int] = None
	source: str = 'WWF Terrestrial Ecoregions'
	version: str = '2.0'  # WWF Ecoregions version


class PhenologyMetadata(BaseModel):
	"""Structure for MODIS phenology metadata"""

	phenology_curve: List[int]  # 365-day array (0-255 values)
	source: str = 'MODIS Phenology'
	version: str = '1.0'

	@field_validator('phenology_curve')
	@classmethod
	def validate_curve_length(cls, v: List[int]) -> List[int]:
		"""Validate phenology curve has exactly 366 values"""
		if not v or len(v) != 366:
			raise ValueError('Phenology curve must have exactly 366 values')
		return v


class DatasetMetadata(BaseModel):
	"""Model for the v2_metadata table"""

	dataset_id: int
	metadata: Dict[str, Any]  # Each key is a MetadataType
	version: int
	created_at: Optional[datetime] = None
	processing_runtime: Optional[float] = None

	@field_serializer('created_at', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()


class DatasetAudit(BaseModel):
	"""Model for the dataset_audit table"""

	dataset_id: int
	audit_date: Optional[datetime] = None
	is_georeferenced: Optional[bool] = None
	has_valid_acquisition_date: Optional[bool] = None
	acquisition_date_notes: Optional[str] = None
	has_valid_phenology: Optional[bool] = None
	phenology_notes: Optional[str] = None
	deadwood_quality: Optional[PredictionQualityEnum] = None
	deadwood_notes: Optional[str] = None
	forest_cover_quality: Optional[PredictionQualityEnum] = None
	forest_cover_notes: Optional[str] = None
	aoi_done: Optional[bool] = None
	has_cog_issue: Optional[bool] = None
	cog_issue_notes: Optional[str] = None
	has_thumbnail_issue: Optional[bool] = None
	thumbnail_issue_notes: Optional[str] = None
	audited_by: Optional[str] = None  # UUID as string
	notes: Optional[str] = None

	@field_serializer('audit_date', mode='plain')
	def datetime_to_isoformat(field: datetime | None) -> str | None:
		if field is None:
			return None
		return field.isoformat()
