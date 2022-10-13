import json
import os

import boto3
import pandas as pd

from pystac import Asset, Item
from pystac.media_type import MediaType
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.raster import RasterBand, RasterExtension

from .constants import RASTER_EXTENSIONS, AssetType
from .globals import STAC_BUCKET


def create_raster_item(tile):
    """Creates STAC item and associated asset for a given tile"""
    item = Item(
        id=tile.tile_id,
        geometry=tile.geometry,
        bbox=tile.properties.get("extent"),
        datetime=tile.item_datetime,
        stac_extensions=RASTER_EXTENSIONS,
        properties={},
    )

    item.set_self_href(tile.href)

    projection = ProjectionExtension.ext(item)
    projection.epsg = tile.epsg
    projection.shape = [
        tile.properties.get("height"),
        tile.properties.get("width"),
    ]  # spec specifies shape in Y, X order

    asset_url = f"s3://{tile.bucket}/{tile.tiles_base}/{tile.tile_id}.tif"

    asset = Asset(
        asset_url, title=tile.tile_id, roles=["data"], media_type=MediaType.COG
    )
    raster = RasterExtension.ext(asset)

    if tile.properties.get("bands") is not None:
        raster.bands = [
            RasterBand(
                {
                    "data_type": band["data_type"],
                    "nodata": band["no_data"],
                    "spatial_resolution": tile.properties["pixelxsize"],
                    "statistics": {
                        "minimum": (
                            band.get("stats", {}).get("min")
                            if isinstance(band["stats"], dict)
                            else None
                        ),
                        "maximum": (
                            band.get("stats", {}).get("max")
                            if isinstance(band["stats"], dict)
                            else None
                        ),
                        "stddev": (
                            band.get("stats", {}).get("std_dev")
                            if isinstance(band["stats"], dict)
                            else None
                        ),
                    },
                }
            )
            for band in tile.properties["bands"]
        ]

    projection = ProjectionExtension.ext(asset)
    projection.epsg = int(tile.epsg)
    projection.shape = [
        tile.properties.get("height"),
        tile.properties.get("width"),
    ]  # spec specifies shape in Y, X order

    item.add_asset(key="data", asset=asset)

    return item


def create_raster_collection(dataset, version, assets, version_datetime):

    tile_sets = list(
        filter(
            lambda asset: asset[0] == AssetType.raster_tile_set
            and "zoom" not in asset[1],
            assets,
        )
    )
    if not tile_sets:
        print(f"no tile sets for version {version}")
        return

    tile_set_groups = set([tile_set[1].split("/")[-3] for tile_set in tile_sets])

    collections = {}
    for group in tile_set_groups:
        tiles_root = os.path.dirname(tile_sets[0][1]).split("//")[1]
        bucket = tiles_root.split("/")[0]

        # will expose the compressed gdal-geotiff version of the tilesets
        tiles_base = f"{'/'.join(tiles_root.split('/')[1:-2])}/{group}/gdal-geotiff"
        tiles_key = f"{tiles_base}/tiles.geojson"
        tiles_epsg = tiles_root.split("/")[4].lstrip("epsg-")

        s3_client = boto3.client("s3")
        print("key", tiles_key)
        resp = s3_client.get_object(Key=tiles_key, Bucket=bucket)

        geojson = json.load(resp["Body"])
        tiles_df = pd.DataFrame(geojson["features"])
        items = []
        for _, tile in tiles_df.iterrows():
            collection = ""
            if len(tile_set_groups) > 1:
                collection = f"/{group}"
            tile.tile_id = tile.properties["name"].split("/")[-1].split(".")[0]

            tile.item_datetime = version_datetime
            tile.href = f"https://{STAC_BUCKET}.s3.amazonaws.com/{dataset}/{version}{collection}/{tile.tile_id}.json"
            tile.tiles_base = tiles_base
            tile.bucket = bucket
            tile.epsg = tiles_epsg
            tile_item = create_raster_item(tile)
            items.append(tile_item)
        collections[group] = items

    return collections
