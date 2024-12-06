"""
Script to export all database tables to CSV files.

This script performs the following steps:
1. Initialize logging, load configuration settings, connect to the database.
2. Export each table in the database to a CSV file, save it to a specified directory.

The script is intended to be run as a standalone process for database exports after the database is complete.
"""

from utils.logging_config import setup_logger, finalize_logger
from utils.db_utils import fetch_table_to_dataframe, initialize_db, tables
from utils.file_utils import save_dataframe_to_csv, load_config
from sqlalchemy import select
import logging
import os


def export_all_tables_to_csv(db, metadata, tables, data_path, logger):
    """
    Export all database tables to CSV files to be used by Tableau.

    Args:
        db (SQLAlchemy Engine): Database connection engine.
        metadata (SQLAlchemy MetaData): Metadata object containing the database schema.
        tables (dict): Dictionary of tables to be exported.
        data_path (str): Directory path where CSV files will be saved.
        logger (logging.Logger): Logger instance to log information during the process.

    Returns:
        None
    """

    with db.connect() as conn:
        # Iterates over each table in the dictionary
        for table_name, table in tables.items():
            df = fetch_table_to_dataframe(conn, table)
            file_path = os.path.join(data_path, f"spotify_{table_name}.csv")
            save_dataframe_to_csv(df, file_path)
            logger.info(f"Exported spotify_{table_name} to {file_path}")


def main():
    """Main function to export tables to CSV files."""
    logger = setup_logger(
        "tableau_export_logger", "tableau_export.log", log_level="INFO"
    )
    db, metadata = initialize_db()
    config = load_config()
    data_path = data_path = config["Paths"]["data_path"]

    export_all_tables_to_csv(db, metadata, tables, data_path, logger)

    finalize_logger(logger)


if __name__ == "__main__":
    main()