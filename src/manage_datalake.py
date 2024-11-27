from datetime import datetime, timedelta, timezone
import os
from zoneinfo import ZoneInfo

from azure.storage.filedatalake import (
    DataLakeServiceClient,
    generate_account_sas,
    ResourceTypes,
    AccountSasPermissions,
)
from dotenv import load_dotenv
from rich import print as rprint  # 🧹 Clean-up later


# Load credentials (if needed)
load_dotenv()

# Retrieve useful information
DATALAKE = os.getenv("DATALAKE")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

if not AZURE_STORAGE_ACCOUNT_KEY:
    raise ValueError("⚠️ Azure storage account key must be provided in the .env file!")

# Define the timezone for France
france_tz = ZoneInfo("Europe/Paris")
start_time_france = datetime.now(tz=france_tz) - timedelta(minutes=5)
expiry_time_france = datetime.now(tz=france_tz) + timedelta(hours=2)

# Generate the SAS token with required permissions
sas_token = generate_account_sas(
    account_name=DATALAKE,
    account_key=AZURE_STORAGE_ACCOUNT_KEY,
    resource_types=ResourceTypes(service=True, file_system=True, object=True),
    permission=AccountSasPermissions(read=True, list=True),
    start=start_time_france,
    expiry=expiry_time_france,
    protocol="https",  # Include HTTPS protocol
    # Include IP Address range
)

# rprint(f"{start_time_france = }")
# rprint(f"{expiry_time_france = }")
# rprint(f"{DATALAKE = }")
# rprint(f"{sas_token = }")

# Define the datalake URL
DATALAKE_URL = f"https://{DATALAKE}.dfs.core.windows.net"

# Create the DataLakeServiceClient using the SAS token
datalake_client = DataLakeServiceClient(
    account_url=DATALAKE_URL,
    credential=sas_token,
    # credential=AZURE_STORAGE_ACCOUNT_KEY
)

FILE_SYSTEM_NAME = "data"
DIRECTORY_PATH = "product_eval"

file_system_client = datalake_client.get_file_system_client(
    file_system=FILE_SYSTEM_NAME
)
directory_client = file_system_client.get_directory_client(DIRECTORY_PATH)

# rprint(file_system_client)
# rprint(directory_client)

try:
    paths = directory_client.get_paths()
    rprint(paths)
    rprint(f"Contents of the directory '{DIRECTORY_PATH}':")
    for path in paths:
        print(f"- {path.name}")
except Exception as e:
    print(f"An error occured while listing contents: {e}")
