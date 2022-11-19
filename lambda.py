import data_api_stac.data_api_catalog as catalog_crud
from data_api_stac.globals import logger


def handler(event, context):
    datasets = event["datasets"]

    for dataset in datasets:
        logger.info(f"Updating {dataset} in catalog.")
        catalog_crud.update_catalog(dataset)
