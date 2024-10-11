"""
This module is responsible for setting up and managing a PostgreSQL database to store, process, and export Spotify music data from extended listening history and the Spotify API.

Functions:
- get_db_connection: Establish a connection to the PostgreSQL database using credentials from environment variables.
- initialize_db: Initialize the database connection.
- create_tables: Create the necessary tables in PostgreSQL.
- load_data_to_db: Load the cleaned DataFrame into a PostgreSQL table.
- check_new_tracks_and_artists: Check if there are new tracks or artists in the batch.
- insert_track_artist: Insert a new track artist into the track artists table.
- insert_new_track: Insert a new track into the tracks table.
- insert_album: Insert a new album into the albums table.
- insert_artist: Insert a new artist and their genres into the artists and artist_genre tables.
- update_audio_features: Update audio features for a specific track in the tracks table.
- update_music_listening_history: Update the music listening history table with Spotify artist and album IDs.
- process_duplicate_artists: Process duplicate artist names by appending a numerical suffix to smaller artists.
- fetch_table_to_dataframe: Fetch all rows from a specified SQL table as a DataFrame.
"""
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, BigInteger, String, Text, DateTime, Boolean, Date, Float, PrimaryKeyConstraint, select, insert, update, and_, or_, inspect, func
from sqlalchemy.engine.reflection import Inspector
import psycopg
import configparser
import time
from datetime import datetime
import pandas as pd

def get_db_connection():
    """
    Establish and return a connection to the PostgreSQL database using credentials from environment variables.

    Returns:
        sqlalchemy.engine.base.Engine: The database engine object for PostgreSQL.

    Raises:
        ValueError: If the PostgreSQL password is not found in environment variables.
    """
    load_dotenv()
    postgresql_pw = os.getenv("POSTGRESQL_PW")

    if not postgresql_pw:
        raise ValueError("PostgreSQL password not found in environment variables")

    # Connection string for psycopg2 PostgreSQL driver
    conn_string = f"postgresql+psycopg://postgres:{postgresql_pw}@localhost:5432/spotify"
    return create_engine(conn_string)

def initialize_db():
    """
    Initialize the database connection and reflect its metadata. 

    Returns:
        tuple: A tuple containing the database connection object and the reflected metadata.
    """
    db = get_db_connection()
    metadata = MetaData()

    # Binds the metadata to the engine and reflect the existing database schema
    metadata.bind = db
    metadata.reflect(bind=db)
    return db, metadata

def create_tables(engine, logger):
    """
    Create the necessary tables in PostgreSQL using the given database engine.

    Args:
        engine (Engine): The database engine for PostgreSQL.

    Returns:
        None

    Notes:
        If any table names change, ensure that the tables list and corresponding raw SQL strings in this file are updated accordingly.
    """
    metadata = MetaData()

    # Defines the table schema, only creating if the table doesn't already exist.
    music_listening_history = Table(
        'music_listening_history_test', metadata,
         Column('music_stream_id', BigInteger, primary_key=True, autoincrement=True),
        Column('spotify_artist_id', String(22)),
        Column('spotify_album_id', String(22)),
        Column('time_ended', DateTime),
        Column('ms_played', BigInteger),
        Column('track_name', Text),
        Column('artist_name', Text),
        Column('album_name', Text),
        Column('reason_started', Text),
        Column('reason_ended', Text),
        Column('shuffle', Boolean),
        Column('skipped', Boolean),     
        Column('incognito', Boolean),
        Column('spotify_track_uri', String(36))
              )

    artists = Table(
        'artists_test', metadata,
         Column('spotify_artist_id', String(22), primary_key=True),
        Column('artist_name', Text),
        Column('artist_popularity', Integer),
        Column('followers', BigInteger),
        Column('main_genre', Text),
        Column('genres', Text)
              )
    
    albums = Table(
        'albums_test', metadata,
         Column('spotify_album_id', String(22), primary_key=True),
        Column('spotify_artist_id', String(22)),
        Column('album_name', Text),
        Column('artist_name', Text),
        Column('all_artist_names', Text),
        Column('album_type', Text),
        Column('total_tracks', Integer),
        Column('label', Text),
        Column('release_date', Date),
        Column('release_date_precision', Text)
              )
    
    artist_genres = Table(
        'artist_genre_test', metadata,
        Column('spotify_artist_id', String(22)),
        Column('genre', Text)
              )
    
    tracks = Table(
        'tracks_test', metadata,
        Column('spotify_track_uri', String(36)),
        Column('spotify_track_id', String(22), primary_key=True),
        Column('spotify_artist_id', String(22)),
        Column('spotify_album_id', String(22)),
        Column('track_name', Text),
        Column('all_artist_names', Text),
        Column('duration_ms', Integer),
        Column('track_popularity', Integer),
        Column('acousticness', Float),
        Column('danceability', Float),
        Column('energy', Float),
        Column('instrumentalness', Float),
        Column('liveness', Float),     
        Column('loudness', Float),
        Column('speechiness', Float),
        Column('valence', Float),
        Column('tempo', Float),
        Column('key', Integer),
        Column('time_signature', Integer)
              )
    
    tracks_consolidated = Table(
        'tracks_consolidated_test', metadata,
        Column('spotify_track_uri', String(36)),
        Column('spotify_track_id', String(22), primary_key=True),
        Column('spotify_artist_id', String(22)),
        Column('spotify_album_id', String(22)),
        Column('track_name', Text),
        Column('all_artist_names', Text),
        Column('duration_ms', Integer),
        Column('track_popularity', Integer),
        Column('acousticness', Float),
        Column('danceability', Float),
        Column('energy', Float),
        Column('instrumentalness', Float),
        Column('liveness', Float),     
        Column('loudness', Float),
        Column('speechiness', Float),
        Column('valence', Float),
        Column('tempo', Float),
        Column('key', Integer),
        Column('time_signature', Integer)
              )
    
    track_artists = Table(
        'track_artists_test', metadata,
         Column('spotify_track_uri', String(36)),
        Column('spotify_track_id', String(22)),
        Column('spotify_artist_id', String(22)),
        PrimaryKeyConstraint('spotify_track_id', 'spotify_artist_id', name='track_artist_pk')
              )

    track_mapping = Table(
        'track_mapping_test', metadata,
        Column('old_track_uri', String(36), primary_key=True),
        Column('new_track_uri', String(36))
              )
    
    # Uses the inspector to check existing tables in the database
    inspector = Inspector.from_engine(engine)
    existing_tables = set(inspector.get_table_names())
    
    # Creates the defined tables only if they do not already exist
    for table_name, table in tables.items():
        if table_name in existing_tables:
            logger.info(f"Table '{table_name}' already exists. Skipping creation.")
        else:
            table.create(engine)
            logger.info(f"Table '{table_name}' created in PostgreSQL database")
    
    logger.info("Tables created in PostgreSQL database")


table_db, table_metadata = initialize_db()

# List of all table names in the database to ensure they're handled correctly in the script
# If these table names change, the create_tables function and raw SQL strings in this file will also need to be updated.
table_names = [
        'music_listening_history_test',
        'tracks_test',
        'track_artists_test',
        'artists_test',
        'artist_genre_test',
        'albums_test',
        'track_mapping_test',
        'tracks_consolidated_test'
    ]

# Creates a dictionary to call SQLAlchemy table objects by name.
tables = {name: table_metadata.tables[name] for name in table_names}

# Creates individual SQLAlchemy table objects for each table, to be used across scripts for queries and insertions
music_listening_history_table = tables['music_listening_history_test']
tracks_table = tables['tracks_test']
track_artists_table = tables['track_artists_test']
artists_table = tables['artists_test']
artist_genre_table = tables['artist_genre_test']
albums_table = tables['albums_test']
track_mapping_table = tables['track_mapping_test']
tracks_consolidated_table = tables['tracks_consolidated_test']

def load_data_to_db(df, db, table_name, logger):
    """
    Load the cleaned DataFrame into the specified PostgreSQL table.

    Args:
        df (pd.DataFrame): The DataFrame to load into the database.
        db (Engine): The database connection.
        table_name (str): The name of the table to load data into.

    Returns:
        None
    """
    with db.connect() as conn:
        inspector = inspect(db)
        # Checks if the table already exists
        if table_name in inspector.get_table_names():
            while True:
                user_input = input(f"The table '{table_name}' already exists. Do you want to drop it and proceed? (yes/no): ")
                if user_input.lower() == 'yes':

                    metadata = MetaData()
                    table = Table(table_name, metadata, autoload_with=db)

                    # Clears existing data in the table
                    conn.execute(table.delete())
                    conn.commit()
                    break
                elif user_input.lower() == 'no':
                    logger.info("Operation cancelled by the user.")
                    return
                else:
                    print("Invalid input. Please enter 'yes' or 'no'.")

        # Loads the DataFrame into the specified table
        df.to_sql(table_name, con=conn, if_exists='append', index=False)

    logger.info(f"{table_name} loaded successfully.")

def check_new_tracks_and_artists(conn, batch_track_uris, tracks_table, track_artists_table):
    """
    Check if there are new tracks or track artists in the batch that are not already in the database.

    Args:
        conn: The database connection object.
        batch_track_uris (list): List of Spotify track URIs to check.
        tracks_table: SQLAlchemy Table object representing the tracks table.
        track_artists_table: SQLAlchemy Table object representing the track artists table.

    Returns:
        tuple: A tuple indicating whether there are new tracks and/or new track artists in the batch.
    """
    batch_has_new_tracks = False
    batch_has_new_track_artists = False

    for track_uri in batch_track_uris:
        # Checks if the track already exists in the database
        track_exists = conn.execute(select(tracks_table).where(tracks_table.c.spotify_track_uri == track_uri)).scalar()

        # Checks if the track exists but its audio features are not populated
        af_not_populated = conn.execute(select(tracks_table).where(
                                                and_(
                                                    tracks_table.c.spotify_track_uri == track_uri,
                                                    or_(
                                                        tracks_table.c.acousticness.is_(None),
                                                        tracks_table.c.danceability.is_(None),
                                                        tracks_table.c.energy.is_(None),
                                                        tracks_table.c.instrumentalness.is_(None),
                                                        tracks_table.c.liveness.is_(None),
                                                        tracks_table.c.loudness.is_(None),
                                                        tracks_table.c.speechiness.is_(None),
                                                        tracks_table.c.valence.is_(None),
                                                        tracks_table.c.tempo.is_(None),
                                                        tracks_table.c.key.is_(None),
                                                        tracks_table.c.time_signature.is_(None)
                                                    )
                                                )
                                            )).scalar()

        # Marks batch_has_new_tracks if track is new or audio features are missing
        if not track_exists or af_not_populated:
            batch_has_new_tracks = True

        # Checks if the track artist exists in the database
        track_artist_exists = conn.execute(select(track_artists_table).where(track_artists_table.c.spotify_track_uri == track_uri)).scalar()

        # Marks batch_has_new_track_artists if the track-artist association is new
        if not track_artist_exists:
            batch_has_new_track_artists = True

        # Exits early if both new tracks and new track artists are found
        if batch_has_new_tracks and batch_has_new_track_artists:
            break

    return batch_has_new_tracks, batch_has_new_track_artists

def insert_track_artist(conn, track_artists_table, spotify_track_uri, spotify_track_id, track_artist, logger):
    """
    Insert a new track artist into the track artists table.

    Args:
        conn: The database connection object.
        track_artists_table: SQLAlchemy Table object representing the track artists table.
        spotify_track_uri (str): The Spotify track URI.
        spotify_track_id (str): The Spotify track ID.
        track_artist (dict): Dictionary containing track artist details.

    Returns:
        None
    """
    # Prepares the insert statement SQL for the track artist
    track_artists_insert_stmt = insert(track_artists_table).values(
        spotify_track_uri=spotify_track_uri,
        spotify_track_id=spotify_track_id,
        spotify_artist_id=track_artist['id']
    )
    conn.execute(track_artists_insert_stmt)
    logger.info(f"Track artist: track {spotify_track_id}, artist {track_artist['name']} added")
    

def insert_new_track(conn, tracks_table, track, logger):
    """
    Insert a new track into the tracks table.
    
    Args:
        conn: The database connection object.
        tracks_table: SQLAlchemy Table object representing the tracks table.
        track (dict): Dictionary containing track details.
        
    Returns:
        None
    """
    # Prepares the insert statement SQL for the track
    track_insert_stmt = insert(tracks_table).values(
        spotify_track_uri=track['uri'],
        spotify_track_id=track['id'],
        track_name=track['name'],
        spotify_artist_id=track['artists'][0]['id'],
        spotify_album_id=track['album']['id'],
        track_popularity=track['popularity'],
        duration_ms=track['duration_ms']
    )
    conn.execute(track_insert_stmt)
    logger.info(f"Track: {track['id']} {track['name']} added")

def insert_album(conn, albums_table, album, logger):
    """
    Insert a new album into the albums table.

    Args:
        conn: The database connection object.
        albums_table: SQLAlchemy Table object representing the albums table.
        album (dict): Dictionary containing album details.
    """
    
    try:
        # Parses the release date based on the precision provided by Spotify
        precision_format_map = {"day": "%Y-%m-%d", "month": "%Y-%m", "year": "%Y"}
        release_date = datetime.strptime(album['release_date'], precision_format_map[album['release_date_precision']]).date()
    
        # Prepares the insert statement SQL for the album
        album_insert_stmt = insert(albums_table).values(
            album_name=album['name'],
            spotify_album_id=album['id'],
            spotify_artist_id=album['artists'][0]['id'],
            artist_name=album['artists'][0]['name'],
            album_type=album['album_type'],
            total_tracks=album['total_tracks'],
            label=album['label'],
            release_date=release_date,
            release_date_precision=album['release_date_precision']
        )
        conn.execute(album_insert_stmt)
        logger.info(f"Album: {album['name']} added")

    except Exception as e:
        logger.warning(f"Skipping album {album['name']} due to an error: {e}")

def insert_artist(conn, artists_table, artist_genre_table, artist, logger):
    """
    Insert a new artist and their genres into the artists and artist_genre tables.

    Args:
        conn: The database connection object.
        artists_table: SQLAlchemy Table object representing the artists table.
        artist_genre_table: SQLAlchemy Table object representing the artist genres table.
        artist (dict): Dictionary containing artist details.

    Returns:
        None
    """
    # Prepares the insert statement SQL for the artist
    artist_insert_stmt = insert(artists_table).values(
        artist_name=artist['name'],
        spotify_artist_id=artist['id'],
        artist_popularity=artist['popularity'],
        followers=artist['followers']['total'],
        main_genre=artist['genres'][0] if artist['genres'] else None
    )
    conn.execute(artist_insert_stmt)
    logger.info(f"Artist: {artist['id']} {artist['name']} added")

    # Inserts genres associated with the artist
    if isinstance(artist['genres'], list):
        for genre in artist['genres']:
            genre_insert_stmt = insert(artist_genre_table).values(
                spotify_artist_id=artist['id'],
                genre=genre
            )
            conn.execute(genre_insert_stmt)
            logger.info(f"{artist['id']} {artist['name']} genre: {genre} added")

def update_audio_features(conn, tracks_table, audio_features):
    """
    Update audio features for a specific track in the tracks table.

    Args:
        conn: The database connection object.
        audio_features (dict): Dictionary containing audio feature details.
        tracks_table: SQLAlchemy Table object representing the tracks table.

    Returns:
        None
    """
    # Extracts audio feature values from the input dictionary
    uri = audio_features.get('uri')
    acousticness = audio_features.get('acousticness')
    danceability = audio_features.get('danceability')
    energy = audio_features.get('energy')
    instrumentalness = audio_features.get('instrumentalness')
    liveness = audio_features.get('liveness')
    loudness = audio_features.get('loudness')
    speechiness = audio_features.get('speechiness')
    valence = audio_features.get('valence')
    tempo = audio_features.get('tempo')
    key = audio_features.get('key')
    time_signature = audio_features.get('time_signature')

    # Prepares the update statement SQL for audio features
    audio_features_updt_stmt = update(tracks_table).where(tracks_table.c.spotify_track_uri == uri).values(
        acousticness=acousticness,
        danceability=danceability,
        energy=energy,
        instrumentalness=instrumentalness,
        liveness=liveness,
        loudness=loudness,
        speechiness=speechiness,
        valence=valence,
        tempo=tempo,
        key=key,
        time_signature=time_signature
    )

    conn.execute(audio_features_updt_stmt)

def update_music_listening_history(conn, music_listening_history_table, tracks_table, logger):
    """
    Update the music listening history table with Spotify artist and album IDs that were not available in the initial file from Spotify.

    Args:
        conn: The database connection object.
        music_listening_history_table: SQLAlchemy Table object representing the music listening history table.
        tracks_table: SQLAlchemy Table object representing the tracks table.

    Returns:
        None
    """
    # Prepares the update statement SQL to set artist and album IDs in the listening history
    mlh_update_stmt = update(music_listening_history_table).where(
        music_listening_history_table.c.spotify_track_uri == tracks_table.c.spotify_track_uri
    ).values(
        spotify_artist_id=tracks_table.c.spotify_artist_id,
        spotify_album_id=tracks_table.c.spotify_album_id
    )
    conn.execute(mlh_update_stmt)
    conn.commit()
    logger.info('Music listening history updated with Spotify Artist/Album IDs')
    
def process_duplicate_artists(conn, music_listening_history_table, artists_table, logger):
    """
    Process duplicate artist names by appending a numerical suffix to artists with the same name and less followers. Update all places artist name appears in the database. 

    Args:
        conn: The database connection object.
        music_listening_history_table: SQLAlchemy Table object representing the music listening history table.
        artists_table: SQLAlchemy Table object representing the artists table.
        artist_genre_table: SQLAlchemy Table object representing the artist genres table.

    Returns:
        None

    Notes:
        This cannot be run on partially loaded data. All data must be processed an loaded to the database first.
    """
    # Finds duplicate artist names in the artists table
    dupe_query = select(artists_table.c.artist_name).\
            group_by(artists_table.c.artist_name).\
            having(func.count() > 1)
    dupe_result = conn.execute(dupe_query)
    duplicate_artist_names = [row.artist_name for row in dupe_result]
    logger.error(f"Failed to process album {album_name}: {e}")

    # Queries the database for follower count of duplicate artist names
    query = select(artists_table.c.artist_name, artists_table.c.spotify_artist_id, artists_table.c.followers).where(artists_table.c.artist_name.in_(duplicate_artist_names))
    result = conn.execute(query)

    # Creates a DataFrame from the query result
    dupe_artist_df = pd.DataFrame(result, columns=['artist_name', 'spotify_artist_id', 'followers'])

    # Sorts the DataFrame by followers count in descending order
    dupe_artist_df.sort_values(by='followers', ascending=False, inplace=True)

    artist_counts = {}
    new_artist_names = []

    for index, row in dupe_artist_df.iterrows():
        artist_name = row['artist_name']
        if artist_name in artist_counts:
            # Increments count for duplicate artist names
            artist_counts[artist_name] += 1
            new_artist_name = f'{artist_name} ({artist_counts[artist_name]})'
        else:
            artist_counts[artist_name] = 1
            new_artist_name = artist_name

        new_artist_names.append(new_artist_name)

    # Updates the 'artist_name' column in the dupe_artist_df DataFrame
    dupe_artist_df['artist_name'] = new_artist_names
    
    # Updates the database with new artist names
    for index, row in dupe_artist_df.iterrows():
        artist_name = row['artist_name']
        spotify_artist_id = row['spotify_artist_id']

        # Prepares SQL update statements for music listening history and artists tables
        mlh_dupe_artist_stmt = (update(music_listening_history_table).where(music_listening_history_table.c.spotify_artist_id == spotify_artist_id).values(artist_name=artist_name))
        artists_dupe_artist_stmt = (update(artists_table).where(artists_table.c.spotify_artist_id == spotify_artist_id).values(artist_name=artist_name))
    
        conn.execute(mlh_dupe_artist_stmt)
        conn.execute(artists_dupe_artist_stmt)
        conn.commit()
        logger.info(f"Duplicate artist names handled: {row['artist_name']}")

def fetch_table_to_dataframe(conn, table):
    """
    Fetch all rows from a specified SQL table and return them as a DataFrame.

    Args:
        conn: The database connection object. This should be an active connection to the SQL database.
        table: The SQLAlchemy Table object representing the table to be queried. This table should be already defined and bound to the connection.

    Returns:
        DataFrame: A DataFrame containing the rows from the specified table. The DataFrame will have columns corresponding to the table's columns.
    """
    # Prepares and executes the query to select all rows from the table
    query = select(table)
    result = conn.execute(query)

    # Create a DataFrame from the query results
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def get_artists_and_albums_for_img(db, table):
    """
    Fetch frequently occurring artists and albums from the database.

    Args:
        db (Engine): Database connection engine.
        table (Table): SQLAlchemy Table object to query.

    Returns:
        Tuple[ResultProxy, ResultProxy]: 
            - artists_result: Query result with Spotify artist IDs and names 
              for artists with over 100 occurrences.
            - albums_result: Query result with Spotify album IDs, album names, 
              and artist names for albums with over 40 occurrences.
    """
    
    with db.connect() as conn:
        # Query for artists with more than 100 occurrences
        artists_query = select(table.c.spotify_artist_id, table.c.artist_name).where(
            table.c.spotify_artist_id.isnot(None)
        ).group_by(table.c.spotify_artist_id, table.c.artist_name).having(func.count() >= 100)

        # Query for albums with more than 40 occurrences
        albums_query = select(table.c.spotify_album_id, table.c.album_name, table.c.artist_name).where(
            table.c.spotify_album_id.isnot(None)
        ).group_by(table.c.spotify_album_id, table.c.album_name, table.c.artist_name).having(func.count() >= 40)

        artists_result = conn.execute(artists_query)
        albums_result = conn.execute(albums_query)
        
    return artists_result, albums_result
        

# If these table names are changed, the create_tables function and table_names list should be updated.
TRACK_MAPPING_QUERY = """
TRUNCATE TABLE track_mapping_test;

INSERT INTO track_mapping_test
SELECT COALESCE(y.spotify_track_uri,x.spotify_track_uri) old_track_uri,
	   x.spotify_track_uri AS new_track_uri FROM
   ( SELECT 
        t.spotify_track_uri,
		t.spotify_track_id,
		t.spotify_artist_id,
		t.spotify_album_id,
        t.track_name,
        t.duration_ms,
        t.track_popularity,
        t.acousticness,
        t.danceability,
        t.energy,
        t.instrumentalness,
        t.liveness,
        t.loudness,
        t.speechiness,
        t.valence,
        t.tempo,
        t.key,
        t.time_signature,
		t.duration_ms_group,
        ROW_NUMBER() OVER (
            PARTITION BY t.spotify_artist_id, t.track_name, t.duration_ms_group
            ORDER BY 
                CASE 
                    WHEN a.album_type = 'album' THEN 1 
                    WHEN a.album_type = 'single' THEN 2 
                    WHEN a.album_type = 'compilation' THEN 3 
                    ELSE 4 
                END, 
                t.track_popularity DESC, 
                a.release_date, 
                t.spotify_track_id
        ) AS rnk
    FROM (
        SELECT 
            t1.*,
            MIN(t2.duration_ms) AS min_duration_ms,
            -- Create a group by minimum duration to ensure tracks within 3000ms are grouped together
            t1.duration_ms - t1.duration_ms % 3000 AS duration_ms_group
        FROM tracks t1
        JOIN tracks t2 
            ON t1.track_name = t2.track_name 
            AND t1.spotify_artist_id = t2.spotify_artist_id
            AND ABS(t1.duration_ms - t2.duration_ms) <= 3000
        GROUP BY 
            t1.spotify_track_uri,
			t1.spotify_track_id,
			t1.spotify_artist_id,
			t1.spotify_album_id,
	        t1.track_name,
	        t1.duration_ms,
	        t1.track_popularity,
	        t1.acousticness,
	        t1.danceability,
	        t1.energy,
	        t1.instrumentalness,
	        t1.liveness,
	        t1.loudness,
	        t1.speechiness,
	        t1.valence,
	        t1.tempo,
	        t1.key,
	        t1.time_signature
    ) t
    JOIN albums a ON t.spotify_album_id = a.spotify_album_id
	) x
LEFT OUTER JOIN
tracks y
ON x.spotify_artist_id = y.spotify_artist_id
AND x.track_name = y.track_name
AND x.duration_ms_group = y.duration_ms - y.duration_ms % 3000
WHERE RNK = 1;
"""

TRACKS_CONSOLIDATED_QUERY = """
TRUNCATE TABLE tracks_consolidated_test;

INSERT INTO tracks_consolidated_test
SELECT DISTINCT
		t.spotify_track_uri,
		t.spotify_track_id,
		t.spotify_artist_id,
		t.spotify_album_id,
        t.track_name,
        an.artist_names,
        t.duration_ms,
        t.track_popularity,
        t.acousticness,
        t.danceability,
        t.energy,
        t.instrumentalness,
        t.liveness,
        t.loudness,
        t.speechiness,
        t.valence,
        t.tempo,
        t.key,
        t.time_signature
FROM tracks t
JOIN track_mapping tm
	ON t.spotify_track_uri = tm.new_track_uri
JOIN 
	(SELECT ta.spotify_track_id, STRING_AGG(a.artist_name,', ' ORDER BY CASE WHEN ta.spotify_artist_id = t.spotify_artist_id THEN 1 ELSE 2 END) artist_names 
	FROM track_artists ta
	JOIN artists a
		ON ta.spotify_artist_id = a.spotify_artist_id
	JOIN tracks t
		ON ta.spotify_track_id = t.spotify_track_id
	GROUP BY ta.spotify_track_id) an
ON t.spotify_track_id = an.spotify_track_id;
"""