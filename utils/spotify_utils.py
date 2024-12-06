"""
This module provides utility functions for interacting with and extracting data from the Spotify API, including retry logic for API calls.
It leverages the Spotipy library and a centralized retry mechanism to handle transient failures when making API requests.

Functions:
- get_spotify_client: Initialize and return a Spotify client.
- retry_operation: A decorator to retry a function execution upon failure.
- fetch_batch_tracks: Fetch track data for a batch of track URIs.
- fetch_audio_features: Fetch audio features for a batch of track URIs.
- fetch_album: Fetch album data based on an album URI.
- fetch_artist: Fetch artist data based on an artist URI.
- process_image: Fetch an image URL from a Spotify request and download the image.
"""

import logging
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine,
    select,
    insert,
    update,
    func,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    and_,
    or_,
)
import time
import functools
import requests
import urllib


class MaxRetriesExceededException(Exception):
    """Raised when the maximum retries for an operation are exceeded."""

    pass


# Centralized retry configuration used by the retry_operation decorator when an API call fails
RETRY_CONFIG = {
    "max_retries": 3,  # Maximum number of retries for API calls
    "delay": 10,  # Delay in seconds between retries
}

# How long to sleep after any API call, including successful ones
sleep_seconds = 5

load_dotenv()


def get_spotify_client():
    """Initialize and return a Spotify client using credentials stored in environment variables.

    Returns:
        None

    Raises:
        ValueError: If Spotify credentials are not found in environment variables.
    """
    spotify_id = os.getenv("SPOTIFY_KEY")
    spotify_secret = os.getenv("SPOTIFY_SECRET")

    if not spotify_id or not spotify_secret:
        raise ValueError("Spotify credentials not found in environment variables.")

    session = requests.Session()

    # Initializes Spotipy client with Spotify credentials
    client_credentials_manager = SpotifyClientCredentials(
        requests_session=session, client_id=spotify_id, client_secret=spotify_secret
    )
    # Disables retries at the Spotipy level, retry logic will be handled in this module
    return spotipy.Spotify(
        retries=0, client_credentials_manager=client_credentials_manager
    )


def retry_operation(func):
    """
    A decorator to retry a function execution upon failure, with a specified number of retries and delay between attempts.

    The retry configuration is retrieved from a global `RETRY_CONFIG` dictionary, which can contain:
    - 'max_retries': The maximum number of retry attempts (default is 3).
    - 'delay': The delay in seconds between retries (default is 5 seconds).

    Args:
        func (Callable): The function to be wrapped and retried upon failure.

    Returns:
        Callable: The wrapped function with retry logic applied.

    Raises:
        Exception: Re-raises the last caught exception after the maximum number of retries is reached.
    """

    @functools.wraps(func)
    def wrapper(logger, *args, **kwargs):
        max_retries = RETRY_CONFIG.get("max_retries", 3)
        delay = RETRY_CONFIG.get("delay", 10)
        attempt = 0
        # Retry loop for the function
        while attempt < max_retries:
            try:
                return func(logger, *args, **kwargs)
            except Exception as e:
                attempt += 1
                if attempt >= max_retries:
                    # Raise an exception if retries exceeded
                    logger.error(
                        f"Max retries reached for {func.__name__}. Raising exception."
                    )
                    raise MaxRetriesExceededException(
                        f"Max retries reached for {func.__name__}"
                    )
                logger.warning(f"Retrying {func.__name__} in {delay} seconds...")
                time.sleep(delay)

    return wrapper


@retry_operation
def fetch_batch_tracks(logger, sp, batch_track_uris):
    """ "
    Make API call to Spotify, return dictionary of track data for all
    tracks in batch.
    """
    logger.info("About to make a tracks call")
    tracks = sp.tracks(batch_track_uris)
    # Pauses to avoid hitting API rate limits
    time.sleep(sleep_seconds)
    return tracks


@retry_operation
def fetch_audio_features(logger, sp, batch_track_uris):
    """Make API call to Spotify, return dictionary of audio features for all tracks in batch."""
    logger.info("About to make an audio features call")
    af = sp.audio_features(batch_track_uris)
    # Pauses to avoid hitting API rate limits
    time.sleep(sleep_seconds)
    return af


@retry_operation
def fetch_album(logger, sp, album_uri):
    """Make API call to Spotify on album URI, return dictionary of album data."""
    logger.info("About to make an album call")
    album = sp.album(album_uri)
    # Pauses to avoid hitting API rate limits
    time.sleep(sleep_seconds)
    return album


@retry_operation
def fetch_artist(logger, sp, artist_uri):
    """Make API call to Spotify on artist URI, return dictionary of artist data."""
    logger.info("About to make an artist call")
    artist = sp.artist(artist_uri)
    # Pauses to avoid hitting API rate limits
    time.sleep(sleep_seconds)
    return artist


def process_image(entity, entity_name, local_path, sp_func):
    """
    Retrieve an entity's image URL using a provided Spotify function (`sp_func`),
    then download and save the image locally to `local_path`. Pause execution for a
    specified number of seconds after downloading the image.

    Args:
        entity: The Spotify-returned entity (e.g., artist, album) for which the image is being processed.
        entity_name: The name of the entity (used for logging and identification).
        local_path (str): The local file path where the image will be saved.
        sp_func: A function that retrieves the entity's data (e.g., `spotify.artist()`).

    Returns:
        None
    """
    entity_data = sp_func(entity)
    # Extract the first image URL
    image_url = entity_data["images"][0]["url"]
    # Download the image to the specified local path
    urllib.request.urlretrieve(image_url, local_path)
    # Pause to avoid hitting API rate limits
    time.sleep(sleep_seconds)
