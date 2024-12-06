"""
This module provides utility functions for interacting with and extracting data from the Spotify API, including retry logic for API calls.
It leverages the Spotipy library and a centralized retry mechanism to handle transient failures when making API requests.

Functions:
- setup_logger: Sets up the logger configuration.
"""

import logging
import os
from datetime import datetime


def setup_logger(name, log_file, log_level="DEBUG"):
    """
    Set up a logger that logs to both a file and the console.

    Args:
        name (str): The name of the logger.
        log_file (str): The name of the log file (will be saved in a 'logs' directory one level above).
        log_level (str): The log level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Create the logs directory path
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")
    os.makedirs(logs_dir, exist_ok=True)  # Create logs directory if it doesn't exist

    # Construct full path for the log file
    log_file_path = os.path.join(logs_dir, log_file)

    logger = logging.getLogger(name)

    # Check if the logger is already set up to avoid duplicates
    if not logger.hasHandlers():

        # Convert log level string to actual logging level
        level = getattr(logging, log_level.upper(), logging.DEBUG)
        logger.setLevel(level)

        # Create file handler
        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)  # Always log everything to the file
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)  # Set console level based on the parameter
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        logger.info(
            "======== Start of Run: {timestamp} =======".format(
                timestamp=datetime.now()
            )
        )

    return logger


def finalize_logger(logger):
    """Logs the end time and closes all handlers for the logger."""
    logger.info(
        "======== End of Run: {timestamp} =======".format(timestamp=datetime.now())
    )
    for handler in logger.handlers[
        :
    ]:  # Use a slice to avoid modifying the list during iteration
        handler.close()
        logger.removeHandler(handler)
