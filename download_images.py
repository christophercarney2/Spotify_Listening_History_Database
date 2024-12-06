"""
Script to retrieve artist and album images from Spotify using data from the database.

This script performs the following steps:
1. Initialize logging and loads configuration settings.
2. Connect to the database and retrieve artists and albums that meet the minimum number of streams.
3. Create directories for artists and images if they don't exist.
4. Download artist and album images from Spotify, sanitize names and save them locally.

This script is intended to be run as a standalone process for maintaining artist and album images. It requires all music listening history data to be loaded.
"""
from utils.logging_config import setup_logger, finalize_logger
from utils.file_utils import (
    load_config,
    read_and_process_track_csv,
    create_directories,
    sanitize_name,
)
from utils.db_utils import (
    initialize_db,
    get_artists_and_albums_for_img,
    music_listening_history_table,
)
from utils.spotify_utils import get_spotify_client, process_image
from sqlalchemy import create_engine, select, func, MetaData, Table
import os
import logging


# In[2]:


def get_artists_and_albums_for_img(db, table):
    """
    Query the database for artists and albums that meet the minimum stream criteria for image retrieval.

    Args:
        db (SQLAlchemy Engine): Database connection engine.
        table (SQLAlchemy Table): The music listening history table from which to retrieve data.

    Returns:
        tuple: Two lists containing results for artists and albums, respectively.
    """

    # Query to get artists who have been listened to at least 100 times
    artists_query = (
        select(table.c.spotify_artist_id, table.c.artist_name)
        .where(table.c.spotify_artist_id.isnot(None))
        .group_by(table.c.spotify_artist_id, table.c.artist_name)
        .having(func.count() >= 100)
    )

    # Query to get albums that have been listened to at least 60 times
    albums_query = (
        select(table.c.spotify_album_id, table.c.album_name, table.c.artist_name)
        .where(table.c.spotify_album_id.isnot(None))
        .group_by(table.c.spotify_album_id, table.c.album_name, table.c.artist_name)
        .having(func.count() >= 60)
    )

    # Executes the queries and fetches all results
    with db.connect() as conn:
        artists_result = conn.execute(artists_query).fetchall()
        albums_result = conn.execute(albums_query).fetchall()

    return artists_result, albums_result

def main():
    """Main function to process and save artist and album images from Spotify."""

    logger = setup_logger(
        "download_images_logger", "download_images.log", log_level="INFO"
    )
    config = load_config()
    db, metadata = initialize_db()
    sp = get_spotify_client()
    track_df = read_and_process_track_csv()

    artist_image_directory = config["Paths"]["artist_image_path"]
    album_image_directory = config["Paths"]["album_image_path"]

    # Creates directories for storing images if they don't already exist
    create_directories(logger, artist_image_directory, album_image_directory)

    # Retrieves artist and album data from the database
    artists_result, albums_result = get_artists_and_albums_for_img(
        db, music_listening_history_table
    )

    logger.info(f"Number of artist images found: {len(list(artists_result))}")
    logger.info(f"Number of album images found: {len(list(albums_result))}")

    # Processes and saves artist images
    for row in artists_result:
        try:
            artist_name = sanitize_name(row[1])
            local_path = os.path.join(artist_image_directory, f"{artist_name}.jpg")
            # Processes the image from Spotify's API and save it locally
            process_image(row[0], artist_name, local_path, sp.artist)
            logger.info(f"Artist image for {artist_name} added")
        except Exception as e:
            logger.error(f"Failed to process artist {artist_name}: {e}")

    # Processes and save album images
    for row in albums_result:
        try:
            album_name = sanitize_name(row[1])
            album_artist_name = sanitize_name(row[2])
            local_path = os.path.join(
                album_image_directory, f"{album_artist_name} - {album_name}.jpg"
            )
            # Processes the image from Spotify's API and save it locally
            process_image(row[0], album_name, local_path, sp.album)
            logger.info(f"Album image for {album_name} added")
        except Exception as e:
            logger.error(f"Failed to process album {album_name}: {e}")

    finalize_logger(logger)


if __name__ == "__main__":
    main()