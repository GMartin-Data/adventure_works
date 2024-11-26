from datetime import datetime, timedelta, timezone
import os

from azure.storage.filedatalake import (
    DataLakeServiceClient,
    generate_account_sas,
    ResourceTypes,
    AccountSasPermissions
)
from dotenv import load_dotenv


# Load credentials (if needed)
load_dotenv()

# Retrieve useful information
DATALAKE = os.getenv("DATALAKE")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

if not AZURE_STORAGE_ACCOUNT_KEY:
    raise ValueError("⚠️ Azure storage account key must be provided in the .env file!")

# Generate the SAS token with required permissions
sas_token = generate_account_sas(
    account_name=DATALAKE,
    account_key=AZURE_STORAGE_ACCOUNT_KEY,
    resource_types=ResourceTypes(service=True, file_system=True, object=True),
    permission=AccountSasPermissions(read=True, list=True),
    expiry=datetime.now(tz=timezone.utc) + timedelta(hours=7),
    # Include HTTPS protocol
    # Include IP Address range
)

# Define the datalake URL
DATALAKE_URL = f"https://{DATALAKE}.dfs.core.windows.net"

# Create the DataLakeServiceClient using the SAS token
client = DataLakeServiceClient(
    account_url=DATALAKE_URL,
    credential=sas_token
)
