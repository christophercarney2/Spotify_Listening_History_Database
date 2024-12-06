"""
Script to process and consolidate music listening history data from JSON files provided by Spotify.

Files will need to be named endsong_X.json, where X is the number. Spotify changes file naming conventions periodically, so this is not automated.

This script performs the following steps:
1. Initialize logging and load configuration settings.
2. Check for existing consolidated files and handle user input for overwriting.
3. Iterate through JSON files of music listening history to load and clean data.
4. Save cleaned data to CSV and combine them into a master CSV file.
5. Load the consolidated data into the database.
"""

from utils.logging_config import setup_logger, finalize_logger
from utils.file_utils import (
    load_config,
    get_current_date_string,
    get_file_count,
    load_json_to_dataframe,
    clean_dataframe,
    save_dataframe_to_csv,
    combine_csv_files,
    load_and_clean_csv,
)
from utils.db_utils import initialize_db, load_data_to_db, music_listening_history_table
import logging
import os


def main():
    """Main function to process, consolidate, and load music listening history."""
    logger = setup_logger(
        "combine_load_data_logger", "combine_load_data.log", log_level="INFO"
    )
    config = load_config()

    # Retrieves the current date as a string and get the number of JSON files to process
    date_string = get_current_date_string()
    file_count = get_file_count()

    # Defines the path for the master CSV file based on the data path from the config
    data_path = config["Paths"]["data_path"]
    master_file_path = os.path.join(data_path, f"Spotify_Listening_Data.csv")

    # Checks if the master file already exists, prompting user to delete if so
    if os.path.isfile(master_file_path):
        user_input = input(
            "There is an existing full history file present. Overwrite? (yes/no):"
        )
        if user_input.strip().lower() == "yes":
            os.remove(master_file_path)
            logger.info("Existing file removed.")
        else:
            logger.info("Operation cancelled.")
            finalize_logger(logger)
            return
    # Iterates through the range of JSON files to process
    for loop_number in range(file_count + 1):
        # Constructs the file path for the current JSON file
        input_file_path = os.path.join(data_path, f"endsong_{loop_number}.json")
        logger.info(f"Processing file: {input_file_path}")

        # Loads the JSON data into a DataFrame, drops and renames fields
        df = load_json_to_dataframe(input_file_path)
        df = clean_dataframe(df)

        # If processing the first file, saves it as the master file
        if loop_number == 0:
            save_dataframe_to_csv(df, master_file_path)
        else:
            # For subsequent files, saves to a temporary CSV file and combines
            current_file_path = os.path.join(
                data_path, f"Spotify_Listening_Data_{date_string}_{loop_number}.csv"
            )
            save_dataframe_to_csv(df, current_file_path)
            combine_csv_files(master_file_path, current_file_path)
            # Removes the temporary file after combining
            os.remove(current_file_path)

    logger.info("Data consolidation complete.")

    db, metadata = initialize_db()

    csv_df = load_and_clean_csv(master_file_path)
    load_data_to_db(csv_df, db, music_listening_history_table.name, logger)

    finalize_logger(logger)


if __name__ == "__main__":
    main()
