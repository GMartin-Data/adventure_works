from datetime import datetime, timedelta, timezone
import logging
import os
from typing import List

from azure.storage.blob import (
    generate_container_sas,
    ContainerSasPermissions,
)
from dotenv import load_dotenv


def generate_sas_url() -> None:
    """
    Generate a SAS url and store it within the SAS_URL environment variable.
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
        print(f"❌ SAS Error : {e}")


if __name__ == "__main__":
    generate_sas_url()
