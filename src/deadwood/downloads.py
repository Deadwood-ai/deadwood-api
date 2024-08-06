import zipfile
import io
import json
import tempfile
from pathlib import Path

import geopandas as gpd

from ..models import Metadata, Label


def label_to_geopackage(label_file, label: Label) -> io.BytesIO:
    # create a GeoDataFrame from the label
    label_gdf = gpd.GeoDataFrame.from_features([{
        "type": "Feature", 
        "geometry": label.label.model_dump(), 
        "properties": {"source": label.label_source, "type": label.label_type, "quality": label.label_quality}}
    ])
    label_gdf.set_crs("EPSG:4326", inplace=True)
    label_gdf.to_file(label_file, driver="GPKG", layer="labels")

    # create a layer for the aoi
    aoi_gdf = gpd.GeoDataFrame.from_features([{ "type": "Feature", "geometry": label.aoi.model_dump(), "properties": {"dataset_id": label.dataset_id} }])
    aoi_gdf.set_crs("EPSG:4326", inplace=True)
    aoi_gdf.to_file(label_file, driver="GPKG", layer="aoi")
    
    return label_file


def bundle_dataset(target_path: str, archive_file_path: str, metadata: Metadata, file_name: str | None = None, label: Label | None = None):
    # build the file name
    if file_name is None:
        file_name = Path(metadata.name).stem

    # create the zip archive
    with zipfile.ZipFile(target_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        # add the main file to the archive
        archive.write(archive_file_path, arcname=f"{file_name}.tif")
        
        # add the metadata to the archive
        archive.writestr(f"{file_name}.json", metadata.model_dump_json())
        archive.writestr(f"{file_name}.schema.json", json.dumps(metadata.model_json_schema(), indent=4))
    
    # write the labels into a geopackage
    if label is not None:
        with tempfile.NamedTemporaryFile(suffix=".gpkg") as label_file:
            label_to_geopackage(label_file.name, label)

            with zipfile.ZipFile(target_path, 'a', zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
                archive.write(label_file.name, arcname="labels.gpkg")
    
    return target_path
            
