import logging
import os
from typing import Any

from dotenv import load_dotenv


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


if __name__ == "__main__":
    init_project()
