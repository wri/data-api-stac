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
from typing import Union, Optional

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


def create_catalog(overwrite=False):
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
    except Exception as e:
        logger.warning(
            f"Error enocuntered fetching catalog ${CATALOG_URL} not found creating one: ",
            e,
        )

    if overwrite or catalog is None:
        catalog = Catalog(
            id=CATALOG_NAME,
            description="Global Forest Watch STAC catalog",
            href=CATALOG_URL,
            stac_extensions=stac_extensions,
            catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED,
        )

    datasets = resp.json()["data"]

    # this has not effect if overwrite is `True` since catalog is empty so no need to wrap in if logic
    existing_items = [dataset.id for dataset in catalog.get_children()]
    datasets = [
        dataset for dataset in datasets if dataset["dataset"] not in existing_items
    ]

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
        return

    catalog.add_child(dataset_collection)
    dataset_collection.save_object(stac_io=S3StacIO(), include_self_link=True)

    catalog.save_object(stac_io=S3StacIO())


def create_dataset_version_collection(
    dataset_name: str, version: str, title: str, description: str
) -> Union[None, Collection]:
    version_url = f"{DATA_API_URL}/dataset/{dataset_name}/{version}"
    try:
        resp = requests.get(version_url)
    except Exception as e:
        logger.error("Unable to fetch {dataset_name}:{version} data", e)
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

    # if source_asset_type == AssetType.database_table:
    #     dataset_items = create_tabular_collection(
    #         dataset, version, version_datetime
    #     )
    #     if not dataset_items:
    #         continue

    # if source_asset_type == AssetType.raster_tile_set:
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

    return dataset_collection


def create_dataset_collection(
    dataset_name: str, session=None
) -> Union[None, Collection]:
    """
    Creates STAC collection for a raster dataset with all its versions
    """
    if not session:
        session = requests.Session()
    resp = session.get(f"{DATA_API_URL}/dataset/{dataset_name}")
    if not resp.ok:
        logger.error(f"Dataset {dataset_name} not found")
        return
    dataset_data = resp.json()["data"]
    versions = dataset_data["versions"]

    resp = session.get(f"{DATA_API_URL}/dataset/{dataset_name}/latest")
    if resp.ok:
        latest_version = resp.json()["data"]["version"]
    elif len(versions) > 0:
        logger.warning(f"Dataset {dataset_name} has no latest version")
        latest_version = sorted(versions)[-1]
    else:
        logger.error(f"No versions found for {dataset_name}")
        return

    dataset_collections = OrderedDict()

    included_versions = versions[: versions.index(latest_version) + 1]
    for index, version in enumerate(sorted(included_versions)):
        dataset_collection: Union[Collection, None] = create_dataset_version_collection(
            dataset_name,
            version,
            dataset_data["metadata"]["title"],
            dataset_data["metadata"]["overview"],
        )

        # if source_asset_type in [
        #     AssetType.geo_database_table,
        #     AssetType.database_table,
        # ]:
        #     resp = requests.get(
        #         f"{DATA_API_URL}/dataset/{dataset_name}/{version}/fields"
        #     )
        #     if resp.ok:
        #         fields = resp.json()["data"]
        #         dataset_collection.stac_extensions += TABULAR_EXTENSIONS
        #         table = TableExtension.ext(dataset_collection)
        #         table.columns = [
        #             {
        #                 "name": field["field_name"],
        #                 "description": field["field_description"],
        #                 "type": field["field_type"],
        #             }
        #             for field in fields
        #         ]

        for item in all_items:
            item.save_object(stac_io=S3StacIO(), include_self_link=False)
        dataset_collections[version] = dataset_collection

    if not dataset_collections:
        logger.error(
            f"{dataset_name} does not have any valid assets to create STAC collection for."
        )
        return

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

    # create latest dataset collection from latest version that gets added
    # to the catalog
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


if __name__ == "__main__":
    create_catalog()
