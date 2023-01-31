import logging
import os


logger = logging.getLogger("Data-API-STAC")
logger.setLevel(logging.INFO)

STAC_BUCKET = os.environ["STAC_BUCKET"]
DATA_API_URL = os.environ["DATA_API_URL"]
CATALOG_NAME = "gfw-catalog"
