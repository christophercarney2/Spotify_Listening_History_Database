"""
Script to update the music listening history table to include the artist and album IDs from Spotify. These values are not populated in the files provided by Spotify, and must be accessed via the API. Also handles naturally occurring duplicates.

This script performs the following steps:
1. Initialize logging and establish a connection to the database.
2. Update the music listening history with artist and album ID. 
3. Process duplicate artists, appending a numerical suffix to artists with the same name and less followers throughout the database.
4. Execute queries to update the track mapping and consolidated tracks tables. Combine tracks with the same name but different IDs. The song lengths must be within 3 seconds of each other, and the sort order is album version > single > compilation. 

This script is intended to be run as a standalone process after all data is loaded to complete the database.
"""

from utils.logging_config import setup_logger, finalize_logger
from utils.db_utils import (
    initialize_db,
    update_music_listening_history,
    process_duplicate_artists,
    music_listening_history_table,
    tracks_table,
    artists_table,
    artist_genre_table,
    track_mapping_table,
    tracks_consolidated_table,
    TRACK_MAPPING_QUERY,
    TRACKS_CONSOLIDATED_QUERY,
    GENRES_UPDATE_QUERY
)
import logging
from sqlalchemy import text


def main():
    """Main function to update duplicate artists and tracks in the database."""
    logger = setup_logger("db_updates_logger", "db_updates.log", log_level="INFO")
    db, metadata = initialize_db()

    with db.connect() as conn:
        # Updates the music listening history with artist and album ID
        update_music_listening_history(
            conn, music_listening_history_table, tracks_table, logger
        )

        process_duplicate_artists(
            conn, music_listening_history_table, artists_table, logger
        )

        # Runs SQL from db_utils to create the track mapping and track consolidated tables
        conn.execute(text(TRACK_MAPPING_QUERY))
        logger.info("track_mapping table updated")
        conn.execute(text(TRACKS_CONSOLIDATED_QUERY))
        logger.info("tracks_consolidated table updated")
        conn.execute(text(GENRES_UPDATE_QUERY))
        logger.info("genres field in artists table updated")
        conn.commit()

    finalize_logger(logger)


if __name__ == "__main__":
    main()