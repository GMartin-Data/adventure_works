import os

from azure.storage.blob import ContainerClient
from rich import print as rprint

from sas_url import generate_sas_url


generate_sas_url()

sas_url = os.getenv("SAS_URL")

rprint(sas_url)

container_client = ContainerClient.from_container_url(sas_url)

def get_folder_content(folder_name: str):
    blobs_names = (
        blob.name 
        for blob in container_client.walk_blobs(name_starts_with=folder_name)
    )
    for blob_name in blobs_names:
        rprint(blob_name)
        
get_folder_content("machine_learning/")
