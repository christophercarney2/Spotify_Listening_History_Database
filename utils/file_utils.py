"""
This module provides utilities for creating and cleaning a master file of Spotify listening history out of the standard data they provide, manipulating that file, creating batches of IDs to iterate through for API calls, and exporting DataFrames to csv files.

Functions:
- load_config: Load and return configuration from a specified INI file.
- get_current_date_string: Return the current date formatted as a string (YYYYMMDD).
- get_file_count: Prompt the user for the highest numbered JSON file and return it as an integer.
- load_json_to_dataframe: Load JSON data from a file and return it as a Pandas DataFrame.
- clean_dataframe: Clean and rename the DataFrame columns for readability, return the updated DataFrame.
- save_dataframe_to_csv: Save the DataFrame df to a CSV file at the path provided.
- combine_csv_files: Combine the two specified CSV files into one.
- read_and_process_track_csv: Read a CSV file, select specified columns, group by track URI, and rename columns. Return a DataFrame with unique track URIs and renamed columns.
- load_and_clean_csv: Load data from a CSV file, clean it, and return it as a Pandas DataFrame.
- get_batch_track_uris: Get a Pandas Series of track URIs from the DataFrame.
- create_directories: Take any # of directory paths and create them if they don't exist.
- sanitize_name: Remove specified unwanted characters from a string.
"""
import logging
import configparser
import pandas as pd
import json
from datetime import date,  datetime
import os
from sqlalchemy import MetaData, Table
import string

def load_config(file_path='config.ini'):
    """Load and return configuration from a specified INI file."""
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def get_current_date_string():
    """Return the current date formatted as a string (YYYYMMDD)."""
    today = date.today()
    return today.strftime("%Y%m%d")

def get_file_count():
    """Prompt the user for the highest numbered JSON file and return it as an integer."""
    return int(input("What is the highest numbered json file?" ))

def load_json_to_dataframe(file_path):
    """Load JSON data from a file and return it as a Pandas DataFrame."""
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    df = pd.json_normalize(data)
    return df

def clean_dataframe(df):
    """Clean and rename the DataFrame columns for readability, return the updated DataFrame."""
    df = df.drop(columns=[
        'username', 'platform', 'conn_country', 'ip_addr_decrypted', 
        'user_agent_decrypted', 'offline', 'offline_timestamp'
    ])
    df = df.rename(columns={
        "ts": "Time Ended", "ms_played": "MS Played", "master_metadata_track_name": "Track Name", 
        "master_metadata_album_artist_name": "Artist Name", "master_metadata_album_album_name": "Album Name", 
        "spotify_track_uri": "Spotify Track URI", "episode_name": "Episode Name", 
        "episode_show_name": "Show Name", "spotify_episode_uri": "Spotify Episode URI", 
        "reason_start": "Reason Started", "reason_end": "Reason Ended", "shuffle": "Shuffle?", 
        "skipped": "Skipped?", "incognito_mode": "Incognito?"
    })
    return df

def save_dataframe_to_csv(df, file_path):
    """Save the DataFrame to a CSV file at the path provided."""
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

def combine_csv_files(master_file_path, current_file_path):
    """Combine the two specified CSV files into one."""
    master_df = pd.read_csv(master_file_path, dtype={12: 'boolean'})
    current_df = pd.read_csv(current_file_path, dtype={12: 'boolean'})

    # Concatenates the data and save it back to the master file
    combined_df = pd.concat([master_df, current_df], ignore_index=True)
    save_dataframe_to_csv(combined_df, master_file_path)


def read_and_process_track_csv():
    """
    Read a CSV file, select specific columns, group by track URI, and
    rename columns.

    Returns:
        DataFrame: Processed DataFrame with unique track URIs and
        renamed columns.
    """

    config = load_config(file_path='config.ini')

    # Reads the CSV file and selects the desired columns
    df = pd.read_csv(os.path.join(config['Paths']['data_path'],f"Spotify_Listening_Data.csv"),usecols=['Spotify Track URI', 'Artist Name', 'Album Name'])

    # Groups the DataFrame by 'Spotify Track URI' and selects the first row for each group
    df = df.groupby(['Spotify Track URI']).first().reset_index()

    # Renames columns for consistency with code expectations
    df.columns = ['spotify_track_uri', 'artist_name', 'album_name']

    return df    

def load_and_clean_csv(csv_path):
    """Load data from a CSV file, clean it, and return it as a DataFrame."""
    selected_columns = [
        'Time Ended',
        'MS Played',
        'Track Name',
        'Artist Name',
        'Album Name',
        'Spotify Track URI',
        'Reason Started',
        'Reason Ended',
        'Shuffle?',
        'Skipped?',
        'Incognito?',
        'Spotify Episode URI'
    ]

    # Loads CSV into DataFrame and filters out podcast episode-related rows
    df = pd.read_csv(csv_path, usecols=selected_columns)
    df = df[~df['Spotify Episode URI'].notna()]  

    # Removes full row duplicates
    df = df.drop_duplicates()

    # Drops the 'Spotify Episode URI' column as it's no longer needed
    del df['Spotify Episode URI']

    # Renames columns for consistent processing
    df.columns = [
        'time_ended', 'ms_played', 'track_name', 'artist_name', 'album_name',
        'spotify_track_uri', 'reason_started', 'reason_ended', 'shuffle', 'skipped', 'incognito'
    ]

    # Converts the "time_ended" column to datetime for proper timestamp manipulation
    df['time_ended'] = pd.to_datetime(df['time_ended'])

    return df

def get_batch_track_uris(track_df, start_index, batch_size):
    """
    Retrieve a batch of track URIs from a DataFrame.

    Args:
        track_df (pd.DataFrame): The DataFrame containing track URIs.
        start_index (int): The starting index for the batch.
        batch_size (int): The number of track URIs to retrieve.

    Returns:
        pd.Series: A Series of track URIs from the specified batch.
    """
    # Calculates the end index based on batch size and ensure it doesn't exceed DataFrame length
    end_index = min(start_index + batch_size, len(track_df))
    return track_df['spotify_track_uri'][start_index:end_index]


def create_directories(logger, *directories):
    """Takes any # of directory paths and create them if they don't exist."""
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"{directory} directory created")

def sanitize_name(name):
    """Remove specified unwanted characters from a string and return a translation table."""
    unwanted_chars = '":/\\?'
    translation_table = str.maketrans('', '', unwanted_chars)
    return name.translate(translation_table)