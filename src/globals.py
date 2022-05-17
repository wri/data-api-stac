import logging
import os


logger = logging.getLogger("Data-API-STAC")

STAC_BUCKET = os.environ["STAC_BUCKET"]
DATA_API_URL = os.environ["DATA_API_URL"]
GFW_DATA_API_KEY = os.environ["GFW_DATA_API_KEY"]
CATALOG_NAME = 'gfw-catalog'
