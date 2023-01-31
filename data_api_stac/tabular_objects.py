import requests

from pystac import Asset, Item
from pystac.media_type import MediaType

from .constants import AreaType, GadmAreas, TabularDataType
from .globals import DATA_API_URL, logger, STAC_BUCKET


def create_tabular_item(dataset, version, version_datetime, area_name):
    "Create item for a tabular asset"

    area_url = f"https://api.resourcewatch.org/v2/geostore/admin/{area_name}"
    resp = requests.get(area_url)
    if not resp.ok:
        logger.info(f"Could not fetch geometry for {area_name}")
        return

    area_bbox = resp.json()["data"]["attributes"]["bbox"]
    item = Item(
        id=area_name,
        geometry=None,
        bbox=area_bbox,
        datetime=version_datetime,
        stac_extensions=None,
        properties={},
    )

    item_href = f"https://{STAC_BUCKET}.s3.amazonaws.com/{dataset}/{version}/items/{area_name}.json"
    item.set_self_href(item_href)

    query_string = f"SELECT * from data WHERE iso = '{area_name}'"
    asset_href = f"{DATA_API_URL}/dataset/{dataset}/{version}/query?sql={query_string}"
    asset = Asset(
        asset_href, title=area_name, roles=["data"], media_type=MediaType.JSON
    )

    item.add_asset(asset=asset, key=area_name)

    return item


def create_tabular_collection(dataset, version, version_datetime):
    """Create STAC items for tabular dataset"""
    data_type = dataset.split("_")[-1]
    if data_type not in list(TabularDataType):
        logger.error(f"Dataset type needs to be a member of {list(TabularDataType)}")
        return

    area_type = dataset.split("_")[0]
    if area_type != AreaType.gadm:
        logger.error(
            f"STAC collection creation not implemeted for area type {area_type}"
        )
        return

    if GadmAreas.iso not in dataset:
        logger.error(
            "STAC collection create is not implemented for non-iso GADM areas."
        )
        return

    areas_list_dataset = "__".join(dataset.split("__")[:2] + ["iso_whitelist"])
    query_str = "SELECT * from data"
    resp = requests.get(
        f"{DATA_API_URL}/dataset/{areas_list_dataset}/latest/query",
        params={"sql": query_str},
    )
    if not resp.ok:
        logger.error("Can not find areas to create STAC collection")
        return

    areas = resp.json()["data"]
    area_names = [area[GadmAreas.iso] for area in areas]

    items = []
    for area_name in area_names:
        item = create_tabular_item(dataset, version, version_datetime, area_name)
        items.append(item)

    return items
