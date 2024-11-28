import logging
import os
from typing import Any


def create_logger(
    logger_name: str,
    log_file_path: str, 
    level: int = logging.DEBUG
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
        console_handler = logging.StreamHandler()

        # Create formatter
        formatter = logging.Formatter(
            "{name} - {asctime} - {levelname} - {message}",
            style="{",
            datefmt="%Y-%m-%d %H:%M",
        )

        # Set formatter for handlers
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Set level for handlers
        file_handler.setLevel(level)
        console_handler.setLevel(level)
    
        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger


def get_env(key: str) -> Any:
    """Retrieve an environment variable value if existing."""
    if value := os.getenv(key):
        return value
    raise ValueError(f"⚠️ The environment variable '{key}' is not defined!")
