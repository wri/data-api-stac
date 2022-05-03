import logging
import os


logger = logging.getLogger("datapump")
logger.setLevel(logging.DEBUG)

STAC_BUCKET = os.environ["STAC_BUCKET"]
DATA_API_URL = os.environ["DATA_API_URL"]
CATALOG_NAME = 'gfw-catalog'
