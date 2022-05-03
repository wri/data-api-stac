from enum import Enum


RASTER_EXTENSIONS = [
     'https://stac-extensions.github.io/projection/v1.0.0/schema.json',
     'https://stac-extensions.github.io/raster/v1.0.0/schema.json',
]

DATASET_EXTENSIONS = [
     'https://stac-extensions.github.io/version/v1.0.0/schema.json'
]

class AssetType(str, Enum):
    raster_tile_set = "Raster tile set"
    database_table = "Database table"
    geo_database_table = "Geo database table"
    shapefile = "ESRI Shapefile"
    geopackage = "Geopackage"
    ndjson = "ndjson"
    csv = "csv"
    tsv = "tsv"
    grid_1x1 = "1x1 grid"