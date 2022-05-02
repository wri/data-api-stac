from io import StringIO
import pystac
import boto3
import pandas as pd
import json
from datetime import datetime
import os
import requests
from urllib3.exceptions import HTTPError
from urllib.parse import urlparse

from collections import OrderedDict

from shapely.ops import unary_union
from shapely.geometry import shape

from pystac.extensions.raster import RasterExtension, RasterBand
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.version import VersionExtension
from pystac import Item, Collection, Catalog


STAC_BUCKET = os.environ["STAC_BUCKET"]
DATA_API_URL = os.environ["DATA_API_URL"]
CATALOG_NAME = 'gfw-catalog'

stac_extensions = [
     'https://stac-extensions.github.io/projection/v1.0.0/schema.json',
     'https://stac-extensions.github.io/raster/v1.0.0/schema.json',
     'https://stac-extensions.github.io/version/v1.0.0/schema.json'
]

class S3StacIO:
    """Class with special implementation of STACIO's save_json method to save
    STAC objects in S3"""
    def save_json(self, dest_href, data):
        io = StringIO(json.dumps(data))

        s3_client = boto3.client("s3")
        parsed_url = urlparse(dest_href)
        key = parsed_url.path.lstrip("/")
        s3_client.put_object(
            Body=io.getvalue(),
            Bucket=STAC_BUCKET,
            Key=key
        )


def create_gfw_catalog():
    """
    Creates a static STAC catalog for all GFW raster datasets.
    The dataset and its assets are read from the API and STAC objects
    are saved to S3.
    """

    session = requests.Session()
    resp = session.get(f"{DATA_API_URL}/datasets")
    if not resp.ok:
        raise HTTPError("Datasets not found.")

    catalog = Catalog(
        id=CATALOG_NAME,
        description='Global Forest Watch STAC catalog',
        href=f"https://{STAC_BUCKET}.s3.amazonaws.com/{CATALOG_NAME}.json",
        stac_extensions=stac_extensions,
        catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED
    )
    datasets = resp.json()["data"]

    for dataset in datasets:
        dataset_collection = create_dataset_collection(
            dataset["dataset"], session=session
        )

        if dataset_collection is None:
            continue
        catalog.add_child(dataset_collection)
        dataset_collection.save_object(stac_io=S3StacIO(), include_self_link=True)

    catalog.save_object(stac_io=S3StacIO())


def create_dataset_collection(dataset: str, session=None):
    """
    Creates STAC collection for a raster dataset with all its versions
    """

    s3_client = boto3.client("s3")

    if not session:
        session = requests.Session()
    resp = session.get(f"{DATA_API_URL}/dataset/{dataset}")
    if not resp.ok:
        raise HTTPError(f"Dataset {dataset} not found")
    dataset_data = resp.json()["data"]

    resp = session.get(f"{DATA_API_URL}/dataset/{dataset}/latest")
    if not resp.ok:
        print(f"Dataset {dataset} does not have a latest version")
        return
    latest_version = resp.json()["data"]["version"]

    versions = dataset_data["versions"]
    dataset_collections = OrderedDict()
    dataset_end_datetime = None
    
    included_versions = versions[:versions.index(latest_version) + 1]
    for index, version in enumerate(sorted(included_versions)):
        version_url = f"{DATA_API_URL}/dataset/{dataset}/{version}"
        resp = session.get(version_url)
        if not resp.ok:
            continue
        version_data = resp.json()["data"]
        content_date_range = version_data.get("content_date_range")
        if content_date_range:
            # setting item datetime to content end date. For search, it seems reasonable
            # for example, for getting latest collection
            version_datetime = content_date_range[1]
        else:
            date_str = version.split('.')[0].lstrip("v")
            try:
                version_datetime = datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                print(f"No datetime found for {version}")
                continue

        assets = version_data["assets"]
        if not assets:
            print(f"no assets found for version {version}")
            continue

        tile_sets = list(
            filter(
                lambda asset: asset[0] == "Raster tile set" and "zoom" not in asset[1],
                assets
            )
        )
        if not tile_sets:
            print(f"no tile sets for version {version}")
            continue

        tiles_root = os.path.dirname(tile_sets[0][1]).split("//")[1]
        bucket = tiles_root.split("/")[0]

        # will expose the compressed gdal-geotiff version of the tilesets
        tiles_base = "/".join(tiles_root.split('/')[1:-1] + ["gdal-geotiff"])
        tiles_key = f'{tiles_base}/tiles.geojson'
        tiles_epsg = tiles_root.split("/")[4].lstrip("epsg-")

        resp = s3_client.get_object(Key=tiles_key, Bucket=bucket)

        geojson = json.load(resp['Body'])
        tiles_df = pd.DataFrame(geojson['features'])

        version_items = []
        for _, tile in tiles_df.iterrows():
            tile.tile_id = tile.properties['name'].split('/')[-1].split('.')[0]

            tile.item_datetime = version_datetime
            tile.href = f"https://{STAC_BUCKET}.s3.amazonaws.com/{dataset}/{version}/items/{tile.tile_id}.json"
            tile.tiles_base = tiles_base
            tile.bucket = bucket
            tile.epsg = tiles_epsg
            tile_item = create_raster_item(tile)
            version_items.append(tile_item)

        dataset_collection = Collection(
            id=dataset,
            title=dataset_data["metadata"]["title"],
            description=dataset_data["metadata"]["overview"],
            extent=pystac.collection.Extent(
                spatial=get_spatial_extent(version_items),
                temporal=pystac.collection.TemporalExtent(
                    intervals=[[version_datetime, version_datetime]]  # FIXME need to populate with actual start date
                )
            ),
            stac_extensions=stac_extensions
        )
        dataset_collection.set_self_href(
            f"https://{STAC_BUCKET}.s3.amazonaws.com/{dataset}/{version}/{version}-collection.json",
        )

        dataset_end_datetime = (
            version_datetime if dataset_end_datetime is None
            else max(dataset_end_datetime, version_datetime)
        )
        dataset_collection.add_items(version_items)
        for item in version_items:
            item.save_object(stac_io=S3StacIO(), include_self_link=False)
        dataset_collections[version] = dataset_collection


    if not dataset_collections:
        print(f"{dataset} does not have any assets to create STAC collection for.")
        return

    for index, (version, collection) in enumerate(dataset_collections.items()):
        version_ext = VersionExtension.ext(collection)
        version_ext.version = version

        if index > 0:
            version_ext.predecessor = list(
                dataset_collections.values()
            )[index - 1].get_self_href()
        if len(dataset_collections) > 1 and index < len(dataset_collections) - 1:
            version_ext.successor = list(
                dataset_collections.values()
            )[index + 1].get_self_href()

        collection.save_object(stac_io=S3StacIO(), include_self_link=True)

    # create latest dataset collection from latest version that gets added
    # to the catalog
    latest_collection = list(dataset_collections.values())[-1].clone()
    latest_collection.set_self_href(
        "/".join(
        latest_collection.get_self_href().split('/')[:-1] + ["collection.json"]
        )
    )

    return latest_collection


def create_raster_item(tile):
    """Creates STAC item and associated asset for a given tile"""
    item = Item(
            id=tile.tile_id,
            geometry=tile.geometry,
            bbox=tile.properties['extent'],
            datetime=tile.item_datetime,
            stac_extensions=stac_extensions,
            properties={}
        )

    item.set_self_href(tile.href)

    projection = ProjectionExtension.ext(item)
    projection.epsg = tile.epsg
    projection.shape = [tile.properties['height'], tile.properties['width']]  # spec specifies shape in Y, X order

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

    return item


def get_spatial_extent(items):
    polygons = [shape(item.geometry) for item in items]
    # Returns a union of the two geojson polygons for each item
    unioned_geometry = unary_union(polygons)
    # Set the bbox to be the bounds of the unified polygon and return the spatial extent of the collection
    return pystac.SpatialExtent(bboxes=[unioned_geometry.bounds])
