from datetime import datetime, timedelta, timezone
import logging
import os
from typing import List

from azure.storage.blob import (
    ContainerClient,
    generate_container_sas,
    ContainerSasPermissions,
)
from dotenv import load_dotenv
from tqdm import tqdm


# Load environment variables
load_dotenv()

# Configuration du répertoire de logs
log_dir = "./logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename="./logs/parquet_extraction.log",
    level=logging.DEBUG,
    format="%(name)s - %(asctime)s - %(levelname)s - %(message)s",
)

# Logger instanciation
logging.getLogger(__name__)


def generate_sas_token(datalake, account_key, container_name):
    try:
        # Generate SAS token
        sas_token = generate_container_sas(
            account_name=datalake,
            account_key=account_key,
            container_name=container_name,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        return sas_token
    except Exception as e:
        logging.error(f"❌ SAS Error : {e}")
        raise


def generate_sas_url() -> str:
    datalake = os.getenv("DATALAKE")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    container = os.getenv("BLOB_CONTAINER")
    sas_token = generate_sas_token(datalake, account_key, container)
    return f"https://{datalake}.blob.core.windows.net/{container}?{sas_token}"


def list_blobs_with_extension(container_url, file_extension="parquet") -> List[str]:
    try:
        container_client = ContainerClient.from_container_url(container_url)
        blobs = container_client.list_blobs()
        blob_names = [
            blob.name for blob in blobs if blob.name.endswith(f".{file_extension}")
        ]
        return blob_names
    except Exception as e:
        logging.error(f"❌ Error when generating blob names' list: {e}")
        raise


def download_parquet_with_sas(container_url, blob_name, download_path):
    try:
        container_client = ContainerClient.from_container_url(container_url)
        blob_client = container_client.get_blob_client(blob_name)

        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        with open(download_path, "wb") as file:
            file.write(blob_client.download_blob().readall())

        logging.debug(f"✅ Downloaded blob: {blob_name} -> {download_path}")
    except Exception as e:
        logging.error(f"❌ Error during download of {blob_name}: {e}")
        raise


if __name__ == "__main__":
    full_url = generate_sas_url()

    blobs_list = list_blobs_with_extension(full_url)
    logging.debug(f"✅ List of parquet files: {blobs_list}")

    for blob in tqdm(blobs_list, desc="Parquet Files Download...", unit="file"):
        download_path = f"./data/{blob}"
        download_parquet_with_sas(full_url, blob, download_path)
