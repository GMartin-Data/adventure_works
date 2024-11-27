from datetime import datetime, timedelta, timezone
import os
from typing import List

from azure.storage.blob import (
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


def get_folder_blobs(container_client, folder: str) -> List[str]:
    """Get the names of the blobs contained within an Azure 'folder'.
    Args:
        container_client: a ContainerClient Azure object
        folder (str): the Azure source folder's name (with ending / if needed)

    Returns:
        List[str]: a list of blob names
    """
    blobs_names = [
        blob.name 
        for blob in container_client.list_blobs(name_starts_with=folder)
        if (not blob.name.endswith(".xlsx")) and ("." in blob.name)
    ]
    return blobs_names


def download_folder_blobs(
    container_client,
    folder: str,
    logger,
) -> None:
    """Download blobs from an Azure source folder to its local counterpart.

    Args:
        container_client: a ContainerClient Azure object
        folder (str): the Azure source folder's name (with ending / if needed)
        logger: the logger instance used
    """

    # Ensure the destination folder exists
    local_base_folder = f"data/"
    os.makedirs(local_base_folder, exist_ok=True)

    # Get the blob names
    blob_names = get_folder_blobs(container_client, folder)

    if not blob_names:
        logger.debug(f"⚠️ No blobs found in folder '{folder}'")
    else:
        logger.debug(f"🔎 Found {len(blob_names)} blobs in folder '{folder}'")
        # Download blobs
        for blob_name in blob_names:
            try:
                components = blob_name.split("/")
                base_folder = components[0]
                file_name = components[-1]
                # Create the local path
                local_path = os.path.join("data", base_folder, file_name)
                # Ensure the local directory exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                logger.debug(f"⬇️ Starting download for: {blob_name}")
                # Get Blob Client for the blob name
                blob_client = container_client.get_blob_client(blob_name)
                # Download the blob content into the local file
                with open(local_path, "wb") as write_file:
                    blob_client.download_blob().readinto(write_file)
                logger.debug(f"✅ Successfully downloaded {blob_name} to {local_path}")
               
            except Exception as e:
                logger.error(f"❌ Error downloading {blob_name}: {e}")
 