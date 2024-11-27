from datetime import datetime, timedelta, timezone
import os
from typing import List

from azure.storage.blob import (
    ContainerClient,
    ContainerSasPermissions,
    generate_container_sas
)
from dotenv import load_dotenv


def generate_sas_url(logger) -> None:
    """Generate a SAS url and store it within the SAS_URL environment variable.

    Args:
        logger: the logger instance used
    """
    # Load environment variables
    load_dotenv()

    DATALAKE = os.getenv("DATALAKE")
    AZURE_ACCOUNT_STORAGE_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    BLOB_CONTAINER = os.getenv("BLOB_CONTAINER")
    try:
        # Generate SAS token
        sas_token = generate_container_sas(
            account_name=DATALAKE,
            account_key=AZURE_ACCOUNT_STORAGE_KEY,
            container_name=BLOB_CONTAINER,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        os.environ["SAS_URL"] = (
            f"https://{DATALAKE}.blob.core.windows.net/{BLOB_CONTAINER}?{sas_token}"
        )
    except Exception as e:
        logger.error(f"❌ SAS Error : {e}")


generate_sas_url()
sas_url = os.getenv("SAS_URL")

container_client = ContainerClient.from_container_url(sas_url)


def get_folder_blobs(
    container_client, folder: str, ext_exclude: str | None = None
) -> List[str]:
    """Get the names of the blobs contained within an Azure 'folder'.
    Args:
        container_client: a ContainerClient Azure object
        folder (str): the Azure source folder's name (with ending / if needed)
        ext_exclude (str | None, optional): a file extension for files to exclude

    Returns:
        List[str]: a list of blob names
    """
    blobs_names = [
        blob.name for blob in container_client.walk_blobs(name_starts_with=folder)
    ]
    if ext_exclude:
        return [
            blob_name
            for blob_name in blobs_names
            if blob_name.endswith(f".{ext_exclude}")
        ]
    return blobs_names


def download_folder_blobs(
    container_client,
    folder: str,
    logger,
    ext_exclude: str | None = None,
) -> None:
    """Download blobs from an Azure source folder to its local counterpart.

    Args:
        container_client: a ContainerClient Azure object
        folder (str): the Azure source folder's name (with ending / if needed)
        logger: the logger instance used
        ext_exclude (str | None, optional): a file extension for files to exclude
    """

    # Ensure the destination folder exists
    os.makedirs(f"data/{folder}/", exist_ok=True)
    
    # Instanciate Blob Client for the source
    blob_client = container_client.get_blob_client(folder)

    # Get the blob names
    blob_names = get_folder_blobs(folder, ext_exclude)

    if not blob_names:
        logger.debug(f"⚠️ No blobs found in folder '{folder}'")
    else:
        logger.debug(f"🔎 Found {len(blob_names)} blobs in folder '{folder}'")
        # Download blobs
        for blob_name in blob_names:
            try:
                local_path = os.path.join(f"data/{folder}", os.path.basename(blob_name))
                logger.debug(f"⬇️ Starting download for: {blob_name}")
                # Download the blob content into the local file
                with open(local_path, "wb") as write_file:
                    blob_client.download_blob().readinto(write_file)
                logger.debug(f"✅ Successfully downloaded {blob_name} to {local_path}")
            except Exception as e:
                logger.error(f"❌ Error downloading {blob_name}: {e}")
