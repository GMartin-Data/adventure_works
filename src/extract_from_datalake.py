from pathlib import Path

from azure.storage.blob import ContainerClient

from datalake_utils import generate_sas_url, download_folder_blobs
from utils import (
    cleanup_file,
    create_logger, 
    extract_tgz_with_progress, 
    get_env, 
    init_project, 
    unzip_archive_with_progress
)


init_project()

LOGS_DIR = get_env("LOGS_DIR")
logger = create_logger(__name__, f"{LOGS_DIR}/extract_from_datalake.log")

# Source environment and generate SAS URL
generate_sas_url(logger)
sas_url = get_env("SAS_URL")

# Instanciate container client
container_client = ContainerClient.from_container_url(sas_url)

# Download blobs
for folder in ("machine_learning/", "nlp_data/", "product_eval/"):
    download_folder_blobs(
        container_client,
        folder,
        logger,
    )
    
# Unzip 
ml_dir = Path("data/machine_learning")
archive_path = ml_dir / "reviews.zip"
extract_to = ml_dir

is_zip_extracted = unzip_archive_with_progress(logger, archive_path, extract_to)

# Delete
if is_zip_extracted:
    cleanup_file(logger, archive_path)

    tgz_path = Path("data/machine_learning/amazon_review_polarity_csv.tgz")
    extract_to = Path("data/machine_learning/amazon_review_polarity_csv")
    extract_to.mkdir(parents=True, exist_ok=True)
    
    is_tgz_extracted = extract_tgz_with_progress(logger, tgz_path, extract_to)
    
    if is_tgz_extracted:
        cleanup_file(logger, tgz_path)
