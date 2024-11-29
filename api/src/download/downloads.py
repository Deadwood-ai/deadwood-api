import zipfile
import io
import json
import tempfile
from pathlib import Path

import geopandas as gpd
import yaml

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


def bundle_dataset(
	target_path: str,
	archive_file_path: str,
	metadata: Metadata,
	file_name: str | None = None,
	label: Label | None = None,
):
	# build the file name
	if file_name is None:
		file_name = Path(metadata.name).stem

	# create the zip archive
	with zipfile.ZipFile(target_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
		# add the main file to the archive
		archive.write(archive_file_path, arcname=f'{file_name}.tif')

		# add the metadata to the archive
		archive.writestr(f'{file_name}.json', metadata.model_dump_json())
		archive.writestr(f'{file_name}.schema.json', json.dumps(metadata.model_json_schema(), indent=4))

	# write the labels into a geopackage
	if label is not None:
		with tempfile.NamedTemporaryFile(suffix='.gpkg') as label_file:
			label_to_geopackage(label_file.name, label)

			with zipfile.ZipFile(target_path, 'a', zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
				archive.write(label_file.name, arcname='labels.gpkg')

	# finally check if some of the extra-files can be provided
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
