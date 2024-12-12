from pydantic_geojson import MultiPolygonModel, PolygonModel


def verify_labels(aoi: PolygonModel, label: MultiPolygonModel) -> bool:
	return True
