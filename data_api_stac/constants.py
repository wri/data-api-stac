from enum import Enum


RASTER_EXTENSIONS = [
d    "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
    "https://stac-extensions.github.io/raster/v1.0.0/schema.json",
]

TABULAR_EXTENSIONS = ["https://stac-extensions.github.io/table/v1.2.0/schema.json"]

DATASET_EXTENSIONS = ["https://stac-extensions.github.io/version/v1.0.0/schema.json"]


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


class AreaType(str, Enum):
    gadm = "gadm"
    wdpa_protected_areas = "wdpa_protected_areas"
    geostore = "geostore"


class GadmAreas(str, Enum):
    iso = "iso"
    adm1 = "adm1"
    adm2 = "adm2"


class TabularDataType(str, Enum):
    alert = "alerts"
    change = "change"
    summary = "summary"
