import requests

import data_api_stac.data_api_catalog as catalog_crud
from data_api_stac.globals import logger, DATA_API_URL


def handler(event, context):
    datasets = event.get("datasets")
    if datasets is None:
        resp = requests.get(f"{DATA_API_URL}/datasets")
        if not resp.ok:
            logger.error("Unable to fetch datasets. Not updating STAC catalog")
            return

    for dataset in datasets:
        logger.info(f"Updating {dataset} in catalog.")
        catalog_crud.update_catalog(dataset)
