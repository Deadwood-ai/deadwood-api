import zipfile
import io
import json

import geopandas as gpd

from ..models import Metadata, Label


def label_to_geopackage(label: Label) -> io.BytesIO:
    # get a buffer to write the geopackage to
    label_file = io.BytesIO()

    # create a GeoDataFrame from the label
    label_gdf = gpd.GeoDataFrame.from_features([{
        "type": "Feature", 
        "geometry": label.label, 
        "properties": {"source": label.label_source, "type": label.label_type, "quality": label.label_quality}}
    ])
    label_gdf.to_file(label_file, driver="GPKG", layer="labels")

    # create a layer for the aoi
    aoi_gdf = gpd.GeoDataFrame.from_features([{ "type": "Feature", "geometry": label.aoi }])
    aoi_gdf.to_file(label_file, driver="GPKG", layer="aoi")

    label_file.seek(0)
    return label_file


def bundle_dataset(target_path: str, archive_file_path: str, metadata: Metadata, file_name: str | None = None, label: Label | None = None):
    # build the file name
    if file_name is None:
        file_name = metadata.name

    # create the zip archive
    with zipfile.ZipFile(target_path, 'w', zipfile.ZIP_DEFLATED) as archive:
        # add the main file to the archive
        with open(archive_file_path, 'rb') as file:
            archive.writestr(f"{file_name}.tif", file.read())
        
        # add the metadata to the archive
        archive.writestr(f"{file_name}.json", metadata.model_dump_json())
        archive.writestr(f"{file_name}.schema.json", json.dumps(metadata.model_json_schema(), indent=4))

        # write the labels into a geopackage
        if label is not None:
            label_gdf = label_to_geopackage(label)
            archive.writestr(f"{file_name}.gpkg", label_gdf.read())
    
    return target_path
            
