import logging
import os
from pathlib import Path
import tarfile
from typing import Any
import zipfile

from dotenv import load_dotenv
from tqdm import tqdm


def create_logger(
    logger_name: str, log_file_path: str, level: int = logging.DEBUG
) -> logging.Logger:
    """Create a custom logger.
    Args:
        logger_name (str): the logger's name
        log_file_path (str): the file path for the file handler
        level (str): the minimal level to log

    Returns:
        a custom logger
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Prevent adding multiple handlers to the logger
    if not logger.hasHandlers():
        # Create handlers
        file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")

        # Create formatter
        formatter = logging.Formatter(
            "{name} - {asctime} - {levelname} - {message}",
            style="{",
            datefmt="%Y-%m-%d %H:%M",
        )

        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)

        logger.addHandler(file_handler)

        return logger


def get_env(key: str) -> Any:
    """Retrieve an environment variable value if existing."""
    if value := os.getenv(key):
        return value
    raise ValueError(f"⚠️ The environment variable '{key}' is not defined!")


def init_project():
    """
    Initialize project with:
    - sourcing environment variables,
    - creating logs and data folders.
    """

    load_dotenv()

    LOGS_DIR = get_env("LOGS_DIR")
    if not os.path.exists(LOGS_DIR):
        os.makedirs(LOGS_DIR)

    DATA_DIR = get_env("DATA_DIR")
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)


def unzip_archive_with_progress(logger, archive_path: Path, extract_to: Path) -> bool:
    """
    Extracts a ZIP archive to a specified directory, with a progress bar

    Args:
        logger: the logger instance used.
        archive_path (Path): Path to the ZIP file.
        extract_to (Path): Directory where files will be extracted.

    Returns:
        bool: True if extraction was successful, False otherwise.
    """
    if not archive_path.is_file():
        logger.error(f"⚠️ The archve file '{archive_path}' does not exist!")
        return False
    
    try:
        with zipfile.ZipFile(archive_path, "r") as zip_read:
            file_list = zip_read.infolist()
            total_files = len(file_list)
            logger.info(f"⏳ Starting extraction of {total_files} files from '{archive_path.name}'")
            
            with tqdm(total=total_files, desc="⛏️ Extracting ZIP", unit="file") as pbar:
                for file_info in file_list:
                    zip_read.extract(file_info, extract_to)
                    pbar.update(1)
        logger.info(f"✅ Successful extracted '{archive_path.name}' to '{extract_to}'.")
        return True
    except zipfile.BadZipFile:
        logger.error(f"❌ The file '{archive_path}' is not a valid archive or is corrupted.")
    except Exception as e:
        logger.error(f"❌ An error occurred while extracting '{archive_path}': {e}")
    return False


def extract_tgz_with_progress(logger, tgz_path: Path, extract_to: Path) -> bool:
    """
    Extracts a TGZ (tar.gz) archive to a specified directory with a progress bar.

    Args:
        logger: the logger instance used.
        tgz_path (Path): Path to the TGZ file.
        extract_to (Path): Directory where files will be extracted.

    Returns:
        bool: True if extraction was successful, False otherwise.
    """
    if not tgz_path.is_file():
        logger.error(f"⚠️ The TGZ file '{tgz_path}' does not exist!")
        return False
    
    try:
        with tarfile.open(tgz_path, "r") as tar_read:
            members = tar_read.getmembers()
            total_members = len(members)
            logger.info(f"⏳ Starting extraction of {total_members} files from '{tgz_path.name}'")
            
            with tqdm(total=total_members, desc="⛏️ Extracting TGZ", unit="file") as pbar:
                for member in members:
                    tar_read.extract(member, extract_to)
                    pbar.update(1)
        
        logger.info(f"✅ Successfully extracted '{tgz_path}' to '{extract_to}'.")
        return True
    except tarfile.ReadError:
        logger.error(f"❌ The file '{tgz_path}' is not a valid TGZ archive or is corrupted.")
    except Exception as e:
        logger.error(f"❌ An error occurred while extracting '{tgz_path}': {e}")   


def cleanup_file(logger, file_path: Path) -> bool:
    """Deletes a file from the filesystem.

    Args:
        logger: the logger instance used.
        file_path (Path): Path to the file to be deleted.

    Returns:
        bool: True if deletion was successful, False otherwise.
    """    
    if not file_path.is_file():
        logger.warning(f"⚠️ The file '{file_path}' does not exist and cannot be deleted.")
        return False
    
    try:
        file_path.unlink()
        logger.info(f"✅ Successfully deleted '{file_path.name}'.")
        return True
    except PermissionError:
        logger.error(f"❌ Permission denied: Cannot delete '{file_path}'.")


if __name__ == "__main__":
    init_project()
