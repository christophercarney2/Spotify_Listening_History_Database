# Project Name

This project includes a script for downloading images from an API.

## Configuration

To set up the script locally:

X. Connection string for Postgres database needs to reflect your actual database. Probably should explain that whole DB setup before tables.
X. Request data from Spotify, move to project directory. 
X. Do I have to explain values to be input for converting Spotify streaming history to single file?
X. Create a `config.ini` file in the root directory (next to `main_script.py`).
X. Define the paths in `config.ini` as follows:

   ```ini
   [Paths]
   image_directory = /path/to/your/base/directory/spotify_artist_images




X. Will need to add a section on generating credentials for Spotify.

X Execution order. 

1. setup_database.py
2. combine_and_load_listening_history.py
3. spotify_api_batch_processing.py
4. db_updates.py
5a. export_files_for_tableau.py*
5b. download_images.py*

* Scripts 5a and 5b can be run in any order.

