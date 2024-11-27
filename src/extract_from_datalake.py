import logging
import os

from azure.storage.blob import ContainerClient

from datalake_utils import generate_sas_url, download_folder_blobs


LOGS_DIRECTORY = "./logs"
if not os.path.exists(LOGS_DIRECTORY):
    os.makedirs(LOGS_DIRECTORY)

# Logger configuration
logging.basicConfig(
    filename="./logs/datalake_extraction.log",
    level=logging.DEBUG,
    format="%(name)s - %(asctime)s - %(levelname)s - %(message)s",
)

# Logger instanciation
logger = logging.getLogger(__name__)

# Source environment and generate SAS URL
generate_sas_url(logger)
sas_url = os.getenv("SAS_URL")

# Instanciate container client
container_client = ContainerClient.from_container_url(sas_url)

for folder in ("machine_learning/", "nlp_data/", "product_eval/"):
    download_folder_blobs(
        container_client,
        folder,
        logger,
    )
