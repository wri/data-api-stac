
from jsonschema import ValidationError
import rasterio
from io import StringIO
import pystac
import boto3
import pandas as pd
import json
import datetime
import os
import requests
from urllib3.exceptions import HTTPError
from urllib.parse import urlparse

from shapely.ops import unary_union
from shapely.geometry import shape

from pystac.extensions.raster import RasterExtension, RasterBand
from pystac.extensions.projection import ProjectionExtension
from pystac import Item
from pystac.stac_io import StacIO

from stac_validator import stac_validator


DATA_LAKE_BUCKET = os.environ["DATA_LAKE_BUCKET"]
DATA_API_URL = os.environ["DATA_API_URL"]
ROOT_DIR = os.environ["STAC_DIRECTORY"]


class S3StacIO(StacIO):
    def save_json(dest_href, data):
        io = StringIO(json.dumps(data))

        s3_client = boto3.client("s3")
        parsed_url = urlparse(dest_href)
        key = parsed_url.path.lstrip("/")
        s3_client.put_object(
            ACL="public-read",
            Body=io.getValue(),
            Bucket=DATA_LAKE_BUCKET,
            Key=key
        )


def create_dataset_collection(dataset: str, version: str):
    """Creates STAC collection of dataset versions"""

    s3_client = boto3.client("s3")

    session = requests.Session()
    resp = session.get(f'{DATA_API_URL}/dataset/{dataset}')
    if not resp.ok:
        raise HTTPError(f"Dataset {dataset} not found")

    versions = resp.json()["versions"]

    for version in versions:
        version_url = f"{DATA_API_URL}/dataset/{dataset}/{version}"
        resp = session.get(version_url)
        if not resp.ok:
            raise HTTPError("Data could not be found.")
        version_data = resp.json()
        content_date_range = version_data.get("content_date_range")
        if not content_date_range:
            raise ValidationError("Content date range is required")

        assets = version_data["assets"]
        if not assets:
            continue

        tile_sets = list(
            assets.filter(
                lambda asset: asset[0] == "Raster tile set" and "zoom" not in asset[1]
            )
        )

        if not tile_sets:
            continue

        tiles_root = os.path.dirname(tile_sets[0][1]).split("//")[1]
        bucket = tiles_root.split("/")[0]

        # will expose the compressed gdal-geotiff version of the tilesets
        tiles_base = "/".join(tiles_root.split('/')[1:-1] + ["gdal-geotiff"])
        tiles_key = f'{tiles_base}/tiles.geojson'
        tiles_epsg = tiles_root.split("/")[4].lstrip("epsg-")

        resp = s3_client.get_object(Key=tiles_key, Bucket=DATA_LAKE_BUCKET)

        geojson = json.load(resp['Body'])
        tiles_df = pd.DataFrame(geojson['features'])

        version_items = []
        for _, tile in tiles_df.iterrows():
            tile.tile_id = tile.properties['name'].split('/')[-1].split('.')[0]

            # setting item datetime to content end date. For search, it seems reasonable
            # for example, for getting latest collection
            tile.item_datetime = content_date_range[1]
            tile.href = f'https://{DATA_LAKE_BUCKET}.s3.amazonaws.com/{ROOT_DIR}/{dataset}/{version}/items/{tile.tile_id}.json'
            tile.tiles_base = tiles_base
            tile.bucket = bucket
            tile.epsg = tiles_epsg
            tile_item = create_tile_item(tile)
            version_items.append(tile_item)


def create_tile_item(tile):
    """Creates STAC item and associated asset for a given tile"""
    item = Item(
            id=tile.tile_id,
            geometry=tile.geometry,
            bbox=tile.properties['extent'],
            datetime=tile.item_datetime,
            href=tile.href,
            properties={}
        )
    projection = ProjectionExtension.ext(item)
    projection.epsg = tile.epsg
    projection.shape = [tile.properties['height'], tile.properties['width'] ]    # spec specifies shape in Y, X order

    asset_url = f'https://{tile.bucket}.s3.amazonaws.com/{tile.tiles_base}/{tile.tile_id}.tif'
    asset = pystac.Asset(
        href=asset_url,
        title=tile.tile_id,
        roles=["data"],
        media_type=pystac.MediaType.COG
    )
    raster = RasterExtension.ext(asset)

    raster.bands = [
        RasterBand(
            {
                'data_type': band['data_type'],
                'nodata': band['no_data'],
                'spatial_resolution': tile.properties['pixelxsize'],
                'statistics': {
                    'minimum': (
                        band.get('stats', {}).get('min')
                        if isinstance(band['stats'], dict) else None
                    ),
                    'maximum': (
                        band.get('stats', {}).get('max')
                        if isinstance(band['stats'], dict) else None
                    ),
                    'stddev': (
                        band.get('stats', {}).get('std_dev')
                        if isinstance(band['stats'], dict) else None
                    )
                }
            }
        ) for band in tile.properties['bands']

    ]

    item.add_asset(key=tile.tile_id, asset=asset)


def get_spatial_extent(items):
    polygons = [shape(item.geometry) for item in items]
    # Returns a union of the two geojson polygons for each item
    unioned_geometry = unary_union(polygons)
    
    # Set the bbox to be the bounds of the unified polygon and return the spatial extent of the collection
    return pystac.SpatialExtent(bboxes=[unioned_geometry.bounds])


