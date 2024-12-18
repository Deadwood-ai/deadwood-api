import zipfile
import io
import json
import tempfile
from pathlib import Path

import geopandas as gpd
import yaml
import pandas as pd

from shared.models import Metadata, Label

TEMPLATE_PATH = Path(__file__).parent / 'templates'


def label_to_geopackage(label_file, label: Label) -> io.BytesIO:
	# create a GeoDataFrame from the label
	label_gdf = gpd.GeoDataFrame.from_features(
		[
			{
				'type': 'Feature',
				'geometry': label.label.model_dump(),
				'properties': {'source': label.label_source, 'type': label.label_type, 'quality': label.label_quality},
			}
		]
	)
	label_gdf.set_crs('EPSG:4326', inplace=True)
	label_gdf.to_file(label_file, driver='GPKG', layer='labels')

	# create a layer for the aoi
	aoi_gdf = gpd.GeoDataFrame.from_features(
		[{'type': 'Feature', 'geometry': label.aoi.model_dump(), 'properties': {'dataset_id': label.dataset_id}}]
	)
	aoi_gdf.set_crs('EPSG:4326', inplace=True)
	aoi_gdf.to_file(label_file, driver='GPKG', layer='aoi')

	return label_file


def create_citation_file(metadata: Metadata, filestream=None) -> str:
	# load the template
	with open(TEMPLATE_PATH / 'CITATION.cff', 'r') as f:
		template = yaml.safe_load(f)

	# fill the template
	template['title'] = f'Deadwood Training Dataset: {metadata.name}'

	# check if the authors can be split into first and last names
	author_list = []
	authors = metadata.authors.split(', ')
	for author in authors:
		author_list.append({'name': author})

	# add all authors defined in the template
	author_list = [*author_list, *template['authors']]

	# check if there is a DOI
	if metadata.citation_doi is not None:
		template['identifiers'] = [
			{'type': 'doi', 'value': metadata.citation_doi, 'description': 'The DOI of the original dataset.'}
		]

	# add the license
	template['license'] = f'{metadata.license.value}-4.0'.upper()

	# create a buffer to write to
	if filestream is None:
		filestream = io.StringIO()
	yaml.dump(template, filestream)

	return filestream


def get_formatted_filename(metadata: Metadata, dataset_id: int, label_id: int = None) -> str:
	"""Generate formatted filename with admin levels and date"""
	# Get admin levels (default to 'unknown' if not set)
	admin1 = metadata.admin_level_1 or 'unknown'
	admin3 = metadata.admin_level_3 or 'unknown'

	# Clean admin names (remove spaces and special chars)
	admin1 = ''.join(c for c in admin1 if c.isalnum())
	admin3 = ''.join(c for c in admin3 if c.isalnum())

	# Format date string
	date_str = f'{metadata.aquisition_year}'
	if metadata.aquisition_month:
		date_str += f'{metadata.aquisition_month:02d}'
	if metadata.aquisition_day:
		date_str += f'{metadata.aquisition_day:02d}'

	# Build base filename
	if label_id:
		return f'labels_{dataset_id}_{admin1}_{admin3}_{label_id}'
	else:
		return f'ortho_{dataset_id}_{admin1}_{admin3}_{date_str}'


def bundle_dataset(
	target_path: str,
	archive_file_path: str,
	metadata: Metadata,
	file_name: str | None = None,
	label: Label | None = None,
):
	# Generate formatted filenames
	base_filename = get_formatted_filename(metadata, metadata.dataset_id)

	# create the zip archive
	with zipfile.ZipFile(target_path, 'w', zipfile.ZIP_STORED) as archive:
		# add the main file to the archive with new name
		archive.write(archive_file_path, arcname=f'{base_filename}.tif')

		# Convert metadata to DataFrame
		df = pd.DataFrame([metadata.model_dump()])

		# Create temporary files for metadata formats
		with tempfile.NamedTemporaryFile(suffix='.csv') as csv_file, tempfile.NamedTemporaryFile(
			suffix='.parquet'
		) as parquet_file:
			# Save metadata in both formats
			df.to_csv(csv_file.name, index=False)
			df.to_parquet(parquet_file.name, index=False)

			# Add both files to archive
			archive.write(csv_file.name, arcname='METADATA.csv')
			archive.write(parquet_file.name, arcname='METADATA.parquet')

	# write the labels into a geopackage if present
	if label is not None:
		label_filename = get_formatted_filename(metadata, metadata.dataset_id, label.id)

		with tempfile.NamedTemporaryFile(suffix='.gpkg') as label_file:
			label_to_geopackage(label_file.name, label)

			with zipfile.ZipFile(target_path, 'a', zipfile.ZIP_STORED) as archive:
				archive.write(label_file.name, arcname=f'{label_filename}.gpkg')

	# Add license if available
	if metadata.license is not None:
		license_file = TEMPLATE_PATH / f'{metadata.license.value.replace(" ", "-")}.txt'
		if license_file.exists():
			with zipfile.ZipFile(target_path, 'a', zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
				archive.write(license_file, arcname='LICENSE.txt')

	# create the citation file
	with tempfile.NamedTemporaryFile('w', suffix='.cff') as citation_file:
		create_citation_file(metadata, citation_file.file)
		with zipfile.ZipFile(target_path, 'a', zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
			archive.write(citation_file.name, arcname='CITATION.cff')

	return target_path
