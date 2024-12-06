"""
Script to process track data from a CSV file, interact with the Spotify API to fetch detailed information about tracks, albums, and artists, 
and insert or update this data into a SQL database. 

This script performs the following steps:
1. Initialize logging and the Spotify API client, connect to the database.
2. Read and process the master CSV file containing listening history data.
3. Fetch new tracks and their corresponding audio features, albums, and artists from Spotify.
4. Insert new records into the relevant database tables while checking for duplicates.

This script is designed to handle batch processing of tracks to efficiently manage API calls and database operations.
It may fail due to the unpredictability of Spotify's rate limiting, but will eventually be able to be restarted.
"""

from utils.logging_config import setup_logger, finalize_logger
from utils.file_utils import read_and_process_track_csv, get_batch_track_uris
from utils.spotify_utils import (
    MaxRetriesExceededException,
    get_spotify_client,
    fetch_batch_tracks,
    fetch_audio_features,
    fetch_album,
    fetch_artist,
)
from utils.db_utils import (
    initialize_db,
    check_new_tracks_and_artists,
    insert_track_artist,
    insert_new_track,
    insert_album,
    insert_artist,
    update_audio_features,
    music_listening_history_table,
    tracks_table,
    track_artists_table,
    artists_table,
    artist_genre_table,
    albums_table,
    track_mapping_table,
    tracks_consolidated_table,
)
from sqlalchemy import select, and_
from datetime import datetime
import logging

def process_batch(
    sp,
    conn,
    start_index,
    batch_track_uris,
    tracks_table,
    track_artists_table,
    artists_table,
    artist_genre_table,
    albums_table,
    logger,
):
    """
    Process a batch of track URIs, checking for new tracks and their respective artists and albums.
    Insert data into respective tables if not already present.

    Args:
        sp (spotipy.Spotify): Spotify client instance.
        conn (Connection): SQLAlchemy connection object.
        start_index (int): The track number in the file to start processing at.
        batch_track_uris (list): List of track URIs to be processed.
        tracks_table (Table): SQLAlchemy Table object for the tracks table.
        track_artists_table (Table): SQLAlchemy Table object for the track_artists table.
        artists_table (Table): SQLAlchemy Table object for the artists table.
        artist_genre_table (Table): SQLAlchemy Table object for the artist_genre table.
        albums_table (Table): SQLAlchemy Table object for the albums table.
        logger (logging.Logger): Logger instance to log information during the process.

    Returns:
        None
    """

    # Checks if there are new tracks or new track artists in the current batch
    batch_has_new_tracks, batch_has_new_track_artists = check_new_tracks_and_artists(
        conn, batch_track_uris, tracks_table, track_artists_table
    )

    if batch_has_new_tracks or batch_has_new_track_artists:
        logger.info(
            f"New tracks or track artists found in batch starting at index {start_index}."
        )
        # Fetches detailed track information from Spotify API for the batch
        batch_tracks = fetch_batch_tracks(logger, sp, batch_track_uris)

        if batch_has_new_track_artists:
            # Inserts new track-artist associations to the database
            handle_new_track_artists(conn, batch_tracks, track_artists_table, logger)

        if batch_has_new_tracks:
            # Fetches audio features from the Spotify API for new tracks in the batch
            af_batch_tracks = fetch_audio_features(logger, sp, batch_track_uris)
            # Inserts new tracks to the database
            handle_new_tracks(
                sp,
                conn,
                batch_tracks,
                af_batch_tracks,
                tracks_table,
                albums_table,
                artists_table,
                artist_genre_table,
                logger,
            )
    else:
        logger.info(
            f"No new tracks or track artists found in batch starting at index {start_index}."
        )


def handle_new_track_artists(conn, batch_tracks, track_artists_table, logger):
    """
    Handle the insertion of new track-artist associations for tracks in a batch.

    Args:
        conn (Connection): SQLAlchemy connection object.
        batch_tracks (dict): Dictionary containing track details from Spotify.
        track_artists_table (Table): SQLAlchemy table object for the 'track_artists' table.
        logger (logging.Logger): Logger instance to log information during the process.

    Returns:
        None
    """

    # Iterates over each track in the batch
    for track in batch_tracks["tracks"]:
        spotify_track_uri = track["uri"]
        spotify_track_id = track["id"]
        track_name = track["name"]
        track_artists = track["artists"]

        # Inserts each new track-artist association into the database
        for track_artist in track_artists:
            insert_track_artist(
                conn,
                track_artists_table,
                spotify_track_uri,
                spotify_track_id,
                track_artist,
                logger,
            )
    conn.commit()
    logger.info("Track artists committed")


def handle_new_tracks(
    sp,
    conn,
    batch_tracks,
    af_batch_tracks,
    tracks_table,
    albums_table,
    artists_table,
    artist_genre_table,
    logger,
):
    """
    Process new tracks and their audio features, albums, and artists. Insert them into the database.

    Args:
        sp (spotipy.Spotify): Spotify client instance.
        conn (Connection): SQLAlchemy connection object.
        batch_tracks (dict): Dictionary containing track details from Spotify.
        af_batch_tracks (list): List of audio features for the tracks.
        tracks_table (Table): SQLAlchemy table object for the 'tracks' table.
        albums_table (Table): SQLAlchemy table object for the 'albums' table.
        artists_table (Table): SQLAlchemy table object for the 'artists' table.
        artist_genre_table (Table): SQLAlchemy table object for the 'artist_genre' table.
        logger (logging.Logger): Logger instance to log information during the process.

    Returns:
        None
    """
    try:
        # Loops over each track and its audio features
        for track, audio_features in zip(batch_tracks["tracks"], af_batch_tracks):
            if track is None:
                logger.warning("Skipping track due to missing data")
                continue

            # Checks if the track is already in the tracks table
            if not conn.execute(
                select(tracks_table).where(
                    tracks_table.c.spotify_track_uri == track["uri"]
                )
            ).scalar():
                insert_new_track(conn, tracks_table, track, logger)

            # Logs if audio features are missing, otherwise inserts/updates them
            if audio_features is None:
                logger.warning(
                    f"Skipping audio feature due to missing data for track: {track.get('id')} {track.get('name', 'Unknown')}"
                )
            else:
                update_audio_features(conn, tracks_table, audio_features)
                logger.info(
                    f"Updated audio features for {track.get('id')} {track.get('name')}"
                )

            # Inserts each new album and artist into the database
            handle_album(sp, conn, track["album"], albums_table, logger)
            handle_artists(
                sp, conn, track["artists"], artists_table, artist_genre_table, logger
            )

        # Commits the transaction only after all operations are complete
        conn.commit()
        logger.info("Tracks, albums, artists, and audio features committed")

    except Exception as e:
        logger.error(
            f"Transaction failed for batch starting at index {start_index}. Error: {e}"
        )
        raise


def handle_album(sp, conn, album, albums_table, logger):
    """
    Handle the insertion of a new album into the 'albums' table.

    Args:
        sp (spotipy.Spotify): Spotify client instance.
        conn (Connection): SQLAlchemy connection object.
        album (dict): Dictionary containing album details from Spotify.
        albums_table (Table): SQLAlchemy table object for the 'albums' table.
        logger (logging.Logger): Logger instance to log information during the process.

    Returns:
        None
    """

    # Checks if the album is already in the albums table
    if not conn.execute(
        select(albums_table).where(
            and_(
                albums_table.c.spotify_artist_id == album["artists"][0]["id"],
                albums_table.c.spotify_album_id == album["id"],
            )
        )
    ).scalar():
        insert_album(conn, albums_table, fetch_album(logger, sp, album["uri"]), logger)


def handle_artists(sp, conn, track_artists, artists_table, artist_genre_table, logger):
    """
    Handle the insertion of new artists into the artists and artist_genre tables.

    Args:
        sp (spotipy.Spotify): Spotify client instance.
        conn (Connection): SQLAlchemy connection object.
        track_artists (list): List of artists associated with a track.
        artists_table (Table): SQLAlchemy table object for the 'artists' table.
        artist_genre_table (Table): SQLAlchemy table object for the 'artist_genre' table.
        logger (logging.Logger): Logger instance to log information during the process.

    Returns:
        None
    """

    for track_artist in track_artists:
        # Checks if the artist is already in the artists table
        if not conn.execute(
            select(artists_table).where(
                artists_table.c.spotify_artist_id == track_artist["id"]
            )
        ).scalar():
            insert_artist(
                conn,
                artists_table,
                artist_genre_table,
                fetch_artist(logger, sp, track_artist["uri"]),
                logger,
            )

def main():
    """
    Main function to handle the batch processing of tracks from CSV data, make relevant Spotify
    API calls and insert the returned data into the appropriate database tables
    """

    logger = setup_logger(
        "spotify_api_batch_logger", "spotify_api_batch.log", log_level="INFO"
    )
    db, metadata = initialize_db()
    sp = get_spotify_client()
    track_df = read_and_process_track_csv()

    # Constants for batch processing
    start_index = int(
        input("Enter the starting index. If this is the first run, enter 0: ")
    )
    batch_size = 50

    try:
        # Processes batches of tracks until all are handled
        while start_index < len(track_df):
            # Retrieves a batch of track URIs from the DataFrame
            batch_track_uris = get_batch_track_uris(track_df, start_index, batch_size)

            with db.connect() as conn:
                # Makes Spotify API calls on batch tracks, insert new data into database
                process_batch(
                    sp,
                    conn,
                    start_index,
                    batch_track_uris,
                    tracks_table,
                    track_artists_table,
                    artists_table,
                    artist_genre_table,
                    albums_table,
                    logger,
                )

                start_index += batch_size

    except MaxRetriesExceededException as e:
        # This will stop the script completely on max retries.
        logger.error(f"Max retries exceeded: {e}")
        raise

    except Exception as e:
        logger.error(f"An error occurred. Batch start index is {start_index}: {e}")

    finally:
        # Ensure the logger is properly closed regardless of outcome
        finalize_logger(logger)


if __name__ == "__main__":
    main()