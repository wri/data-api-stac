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
from shapely.geometry import box, shape

from pystac.extensions.table import Column, TableExtension
from pystac.extensions.version import VersionExtension
from pystac import Collection, Catalog

from .globals import logger, STAC_BUCKET, DATA_API_URL, CATALOG_NAME
from .constants import AssetType, TABULAR_EXTENSIONS
from .raster_objects import create_raster_collection
from .tabular_objects import create_tabular_collection


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
        logger.info(f"Creating STAC collection for {dataset['dataset']}")
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
    if not session:
        session = requests.Session()
    resp = session.get(f"{DATA_API_URL}/dataset/{dataset}")
    if not resp.ok:
        logger.error(f"Dataset {dataset} not found")
        return
    dataset_data = resp.json()["data"]

    resp = session.get(f"{DATA_API_URL}/dataset/{dataset}/latest")
    if not resp.ok:
        logger.error(f"Dataset {dataset} does not have a latest version")
        return
    latest_version = resp.json()["data"]["version"]

    versions = dataset_data["versions"]
    dataset_collections = OrderedDict()

    included_versions = versions[:versions.index(latest_version) + 1]
    for index, version in enumerate(sorted(included_versions)):
        version_url = f"{DATA_API_URL}/dataset/{dataset}/{version}"
        resp = session.get(version_url)
        if not resp.ok:
            logger.error(f"Dataset version {version} not found")
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
                logger.error(f"No datetime found for {version}")
                continue

        assets = version_data["assets"]
        if not assets:
            logger.error(f"no assets found for version {version}")
            continue

        source_asset_type = get_dataset_type(assets)

        supported_types = [AssetType.raster_tile_set, AssetType.database_table]

        if source_asset_type not in supported_types:
            logger.warning(
                f"STAC not implemented for asset type {source_asset_type} yet."
            )
            continue

        if source_asset_type == AssetType.database_table:
            dataset_items = create_tabular_collection(dataset, version, version_datetime)
            if not dataset_items:
                continue

        if source_asset_type == AssetType.raster_tile_set:
            dataset_items = create_raster_collection(
                dataset, version, assets, version_datetime
            )
            if not dataset_items:
                continue

        dataset_collection = Collection(
            id=dataset,
            title=dataset_data["metadata"]["title"],
            description=dataset_data["metadata"]["overview"],
            extent=pystac.collection.Extent(
                spatial=get_spatial_extent(dataset_items),
                temporal=pystac.collection.TemporalExtent(
                    intervals=[[version_datetime, version_datetime]]  # FIXME need to populate with actual start date
                )
            ),
            stac_extensions=stac_extensions
        )
        dataset_collection.set_self_href(
            f"https://{STAC_BUCKET}.s3.amazonaws.com/{dataset}/{version}/{version}-collection.json",
        )

        dataset_collection.add_items(dataset_items)
        if source_asset_type in [
            AssetType.geo_database_table, AssetType.database_table
        ]:
            resp = requests.get(f"{DATA_API_URL}/dataset/{dataset}/{version}/fields")
            if resp.ok:
                fields = resp.json()["data"]
                dataset_collection.stac_extensions += TABULAR_EXTENSIONS
                table = TableExtension.ext(dataset_collection)
                table.columns = [
                    {
                        "name": field["field_name"],
                        "description": field["field_description"]
                    }
                    for field in fields
                ]

        for item in dataset_items:
            item.save_object(stac_io=S3StacIO(), include_self_link=False)
        dataset_collections[version] = dataset_collection

    if not dataset_collections:
        logger.error(
            f"{dataset} does not have any valid assets to create STAC collection for."
        )
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


def get_spatial_extent(items):
    polygons = [
        shape(item.geometry) if item.geometry else box(*item.bbox)
        for item in items
    ]
    # Returns a union of the two geojson polygons for each item
    unioned_geometry = unary_union(polygons)
    # Set the bbox to be the bounds of the unified polygon and return the spatial extent of the collection
    return pystac.SpatialExtent(bboxes=[unioned_geometry.bounds])


def get_dataset_type(assets):
    """Get whether the default assets are of raster, vector or tabular type"""
    if list(
        filter(lambda asset: asset[0] == AssetType.database_table, assets)
    ):
        return AssetType.database_table

    if list(
        filter(lambda asset: asset[0] == AssetType.raster_tile_set, assets)
    ):
        return AssetType.raster_tile_set

    if list(
        filter(lambda asset:  asset[0].lower() == AssetType.geo_database_table, assets)
    ):
        return AssetType.geo_database_table

    logger.error("Did not detect one of the known source asset types")
    return


if __name__ == "__main__":
    create_gfw_catalog()
