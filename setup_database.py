"""
Script to set up the database by creating the required tables. Assumes the tables do not already exist.

This script performs the following steps:
1. Initialize logging.
2. Establish a connection to the database.
3. Create the necessary tables in the database, as defined in the db_utils.py module.

This script is intended to be run as a standalone process for database setup.
"""

from utils.logging_config import setup_logger, finalize_logger
from utils.db_utils import get_db_connection, create_tables
import logging
import os
from datetime import datetime


def main():
    """Main function to configure logging and create the database tables."""
    logger = setup_logger(
        "setup_database_logger", "setup_database.log", log_level="INFO"
    )
    engine = get_db_connection()

    # Creates the tables defined in db_utils
    create_tables(engine, logger)
    logger.info("Database setup complete.")

    finalize_logger(logger)


if __name__ == "__main__":
    main()