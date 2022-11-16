import json
import os
from collections import OrderedDict
from datetime import datetime
from io import StringIO
from ipaddress import collapse_addresses
from typing import Optional, Union
from urllib.parse import urlparse

import boto3
import pandas as pd
import pystac
import requests
from pystac import Catalog, Collection
from pystac.extensions.table import Column, TableExtension
from pystac.extensions.version import VersionExtension
from shapely.geometry import box, shape
from shapely.ops import unary_union
from urllib3.exceptions import HTTPError

from .constants import TABULAR_EXTENSIONS, AssetType
from .globals import CATALOG_NAME, DATA_API_URL, STAC_BUCKET, logger
from .raster_objects import create_raster_collection
from .tabular_objects import create_tabular_collection

stac_extensions = [
    "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
    "https://stac-extensions.github.io/version/v1.0.0/schema.json",
]

# this are temporary mock dates to be able import datasets that don't
# have content_date or content_date_range  in GFW Data API
DATASET_DATETIMES = {
    "umd_tree_cover_loss": datetime(2021, 1, 1),
    "esa_land_cover_2015": datetime(2015, 1, 1),
    "umd_tree_cover_height_2000": datetime(2000, 1, 1),
    "umd_tree_cover_height_2019": datetime(2019, 1, 1),
    "umd_tree_cover_height_2020": datetime(2020, 1, 1),
}

CATALOG_URL = f"https://{STAC_BUCKET}.s3.amazonaws.com/{CATALOG_NAME}.json"


class S3StacIO:
    """Class with special implementation of STACIO's save_json method to save
    STAC objects in S3"""

    def save_json(self, dest_href, data):
        io = StringIO(json.dumps(data))

        s3_client = boto3.client("s3")
        parsed_url = urlparse(dest_href)
        key = parsed_url.path.lstrip("/")
        s3_client.put_object(Body=io.getvalue(), Bucket=STAC_BUCKET, Key=key)


def create_catalog():
    """
    Creates a static STAC catalog for all GFW raster datasets.
    The dataset and its assets are read from the API and STAC objects
    are saved to S3.
    """

    session = requests.Session()
    resp = session.get(f"{DATA_API_URL}/datasets")
    if not resp.ok:
        raise HTTPError("Datasets not found.")

    catalog = None
    try:
        catalog = Catalog.from_file(CATALOG_URL)
    except FileNotFoundError:
        logger.info(f"Catalog {CATALOG_URL} not found so creating one")

    if catalog is not None:
        raise FileExistsError(
            f"Catalog with url {CATALOG_URL} already exists. Check update method."
        )

    catalog = Catalog(
        id=CATALOG_NAME,
        description="Global Forest Watch STAC catalog",
        href=CATALOG_URL,
        stac_extensions=stac_extensions,
        catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED,
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


def update_catalog(dataset_name: str) -> None:
    """Update catalog in dataset"""
    catalog = Catalog.from_file(CATALOG_URL)
    resp = requests.get(f"{DATA_API_URL}/dataset/{dataset_name}")
    if not resp.ok:
        raise HTTPError("Datasets not found.")

    dataset_collection = catalog.get_child(dataset_name)
    if dataset_collection is None:
        logger.warning(
            f"No existing STAC collection for dataset {dataset_name}. Creating new one..."
        )
        dataset_collection = create_dataset_collection(dataset_name)
        if not dataset_collection:
            return
    else:
        dataset_collection = update_dataset_collection(dataset_collection)
        if not dataset_collection:
            return

    catalog.remove_child(dataset_collection.id)
    catalog.add_child(dataset_collection)

    dataset_collection.save_object(stac_io=S3StacIO(), include_self_link=True)
    catalog.save_object(stac_io=S3StacIO())


def create_dataset_version_collection(
    dataset_name: str, version: str, title: str, description: str
) -> Union[None, Collection]:
    version_url = f"{DATA_API_URL}/dataset/{dataset_name}/{version}"
    try:
        resp = requests.get(version_url)
    except Exception as exc:
        logger.error(f"Unable to fetch {dataset_name}:{version} data: {exc}")
        return

    if not resp.ok:
        logger.error(f"Dataset version {dataset_name}:{version} not found")
        return
    version_data = resp.json()["data"]
    version_datetime = version_data.get("content_date")

    if not version_datetime:
        content_date_range = version_data.get("content_date_range")
        if content_date_range:
            # TODO: add this as STAC temporal extent property
            version_datetime = content_date_range[1]
        else:
            version_datetime = DATASET_DATETIMES.get(dataset_name)
            if not version_datetime:
                date_str = version.split(".")[0].lstrip("v")
                try:
                    version_datetime = datetime.strptime(date_str, "%Y%m%d")
                except ValueError:
                    logger.error(f"No datetime found for {dataset_name}:{version}")
                    return

    assets = version_data["assets"]
    if not assets:
        logger.error(f"no assets found for {dataset_name}:{version}")
        return

    source_asset_type = get_dataset_type(assets)

    supported_types = [AssetType.raster_tile_set]

    if source_asset_type not in supported_types:
        logger.warning(f"STAC not implemented for asset type {source_asset_type} yet.")
        return

    try:
        raster_item_groups = create_raster_collection(
            dataset_name, version, assets, version_datetime
        )
    except Exception as e:
        logger.error(f"Encountered error creating {dataset_name}:{version} asset", e)
        return

    if not raster_item_groups:
        return

    all_items = [
        item for collection in raster_item_groups.values() for item in collection
    ]
    dataset_collection = Collection(
        id=dataset_name,
        title=title,
        description=description,
        extent=pystac.collection.Extent(
            spatial=get_spatial_extent(all_items),
            temporal=pystac.collection.TemporalExtent(
                intervals=[
                    [version_datetime, version_datetime]
                ]  # FIXME need to populate with actual start date
            ),
        ),
        stac_extensions=stac_extensions,
    )
    dataset_collection.set_self_href(
        f"https://{STAC_BUCKET}.s3.amazonaws.com/{dataset_name}/{version}/{version}-collection.json",
    )
    if len(raster_item_groups.keys()) > 1:
        raster_collections = []
        for raster_group, items in raster_item_groups.items():
            raster_collection = Collection(
                id=raster_group,
                # title=dataset_data["metadata"]["title"],
                description=description,
                extent=pystac.collection.Extent(
                    spatial=get_spatial_extent(items),
                    temporal=pystac.collection.TemporalExtent(
                        intervals=[
                            [version_datetime, version_datetime]
                        ]  # FIXME need to populate with actual start date
                    ),
                ),
                stac_extensions=stac_extensions,
            )
            raster_collection.add_items(items)
            raster_collections.append(raster_collection)
        dataset_collection.add_children(raster_collections)
        for collection in raster_collections:
            collection.save_object(stac_io=S3StacIO(), include_self_link=True)
    else:
        dataset_collection.add_items(list(raster_item_groups.values())[0])

    for item in all_items:
        item.save_object(stac_io=S3StacIO(), include_self_link=False)

    return dataset_collection


def create_dataset_collection(dataset_name: str) -> Union[None, Collection]:
    """
    Creates STAC collection for a raster dataset with all its versions
    """

    resp = requests.get(f"{DATA_API_URL}/dataset/{dataset_name}")
    if not resp.ok:
        logger.error(f"Dataset {dataset_name} not found")
        return
    dataset_data = resp.json()["data"]
    versions = sorted(dataset_data["versions"])
    if len(versions) == 0:
        logger.warning(f"No dataset versions and assets found for {dataset_name}.")
        return

    latest_version = _get_latest_version(dataset_name)
    if not latest_version:
        logger.warning(
            f"Dataset {dataset_name} has no latest tagged version. Setting more recent version as latest"
        )
        latest_version = sorted(versions)[-1]

    dataset_collections = OrderedDict()
    included_versions = versions[: versions.index(latest_version) + 1]
    for version in included_versions:
        dataset_collection: Union[Collection, None] = create_dataset_version_collection(
            dataset_name,
            version,
            dataset_data["metadata"]["title"],
            dataset_data["metadata"]["overview"],
        )
        if not dataset_collection:
            continue
        dataset_collections[version] = dataset_collection

    if not dataset_collections:
        logger.error(
            f"{dataset_name} does not have any valid assets to create STAC collection for."
        )
        return

    latest_collection = version_and_store_collections(
        dataset_collections, latest_version
    )

    return latest_collection


def update_dataset_collection(dataset_collection):
    resp = requests.get(f"{DATA_API_URL}/dataset/{dataset_collection.id}")
    if not resp.ok:
        logger.error(f"Dataset {dataset_collection.id} not found")
        return
    dataset_data = resp.json()["data"]
    versions = sorted(dataset_data["versions"])

    cat_latest_version = dataset_collection.to_dict()["version"]
    api_latest_version = _get_latest_version(dataset_collection.id)
    if cat_latest_version == api_latest_version:
        logger.info(f"No new versions found for dataset {dataset_collection.id}.")
        return

    start_idx = versions.index(cat_latest_version) + 1

    dataset_collections = OrderedDict()
    new_versions = versions[start_idx:]
    for version in new_versions:
        dataset_version_collection = create_dataset_version_collection(
            dataset_collection.id,
            version,
            dataset_collection.title,
            dataset_collection.description,
        )
        if not dataset_version_collection:
            continue
        dataset_collections[version] = dataset_version_collection

    latest_collection = version_and_store_collections(
        dataset_collections, api_latest_version
    )

    dataset_collection.remove_child(cat_latest_version)
    dataset_collection.add_child(latest_collection)

    return dataset_collection


def version_and_store_collections(dataset_collections, latest_version):
    for index, (version, collection) in enumerate(dataset_collections.items()):
        version_ext = VersionExtension.ext(collection)
        version_ext.version = version

        if index > 0:
            version_ext.predecessor = list(dataset_collections.values())[
                index - 1
            ].get_self_href()
        if len(dataset_collections) > 1 and index < len(dataset_collections) - 1:
            version_ext.successor = list(dataset_collections.values())[
                index + 1
            ].get_self_href()

        collection.save_object(stac_io=S3StacIO(), include_self_link=True)

        if version == latest_version:
            break

    # create latest dataset collection from latest version that gets added
    # to the catalog
    if latest_version:
        latest_collection = dataset_collections[latest_version].clone()
    else:
        latest_collection = list(dataset_collections.values())[-1].clone()

    latest_collection.set_self_href(
        "/".join(
            latest_collection.get_self_href().split("/")[:-1] + ["collection.json"]
        )
    )

    return latest_collection


def get_spatial_extent(items):
    polygons = [
        shape(item.geometry) if item.geometry else box(*item.bbox) for item in items
    ]
    # Returns a union of the two geojson polygons for each item
    unioned_geometry = unary_union(polygons)
    # Set the bbox to be the bounds of the unified polygon and return the spatial extent of the collection
    return pystac.SpatialExtent(bboxes=[unioned_geometry.bounds])


def get_dataset_type(assets):
    """Get whether the default assets are of raster, vector or tabular type"""
    if list(filter(lambda asset: asset[0] == AssetType.database_table, assets)):
        return AssetType.database_table

    if list(filter(lambda asset: asset[0] == AssetType.raster_tile_set, assets)):
        return AssetType.raster_tile_set

    if list(
        filter(lambda asset: asset[0].lower() == AssetType.geo_database_table, assets)
    ):
        return AssetType.geo_database_table

    logger.error("Did not detect one of the known source asset types")
    return


def _get_latest_version(dataset_name: str) -> Union[str, None]:
    resp = requests.get(f"{DATA_API_URL}/dataset/{dataset_name}/latest")
    if not resp.ok:
        logger.warning(f"No dataset version tagged as latest found for {dataset_name}")
        return

    return resp.json()["data"]["version"]


if __name__ == "__main__":
    update_catalog("gfw_integrated_alerts")
