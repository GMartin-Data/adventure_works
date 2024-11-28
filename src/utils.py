import logging
import os
from typing import Any


def create_logger(log_file_path: str, level: int = logging.INFO) -> logging.Logger:
    """Create a custom logger.
    Args:
        log_file_path (str): the file path for the file handler
        level (str): the minimal level to log

    Returns:
        a custom logger
    """
    logger = logging.getLogger(__name__)

    # Handlers
    file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
    console_handler = logging.StreamHandler()

    # Formatter
    formatter = logging.Formatter(
        format="{name} - {asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M",
    )

    # Settings
    file_handler.setLevel(level)
    console_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Bindings
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_env(key: str) -> Any:
    """Retrieve an environment variable value if existing."""
    if value := os.getenv(key):
        return value
    raise ValueError(f"⚠️ The environment variable '{key}' is not defined!")
