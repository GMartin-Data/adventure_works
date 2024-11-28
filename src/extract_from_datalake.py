import os

from azure.storage.blob import ContainerClient

from datalake_utils import generate_sas_url, download_folder_blobs
from utils import create_logger, get_env, init_project


init_project()

# LOGS_DIR = get_env("LOGS_DIR")
# if not os.path.exists(LOGS_DIR):
#     os.makedirs(LOGS_DIR)
LOGS_DIR = get_env("LOGS_DIR")
logger = create_logger(__name__, f"{LOGS_DIR}/datalake_extraction.log")

# Source environment and generate SAS URL
generate_sas_url(logger)
sas_url = get_env("SAS_URL")

# Instanciate container client
container_client = ContainerClient.from_container_url(sas_url)

for folder in ("machine_learning/", "nlp_data/", "product_eval/"):
    download_folder_blobs(
        container_client,
        folder,
        logger,
    )
