
# Spotify Listening History Database Creation
Are you one of those people who get your Spotify Wrapped analysis at the end of every year and think things like "how exactly did they calculate all that?", or "can I drill down any further into this?". This project was designed to do exactly that: construct a functional database of your listening history that lets you answer those questions. Spotify will provide any user with their listening history, but this project will consolidate, clean, expand, and stage that history into a fully explorable dataset. Creating your own version of Wrapped's cleverly shareable screens will then be up to you, but for an example I created a Tableau dashboard that visualizes my own history; judge my questionable taste for yourself [here](https://public.tableau.com/app/profile/chris.carney/viz/MySpotifyHistory10YearsofStreaming/AllStreaming)!

## Overview
This project will:
- Combine and clean listening history json files provided by Spotify
- Upload listening history to a PostgreSQL database
- Use a batch processing mechanism to fetch additional data on tracks, albums, and artists in the listening history from the Spotify API. Load this data into the database
- Handle duplicate artists and tracks based on reasonable logical assumptions
- Export database tables to csv files for use in future projects/visualizations
- Download frequently occurring artist and album images for use in future projects/visualizations

See [Installation](#installation) for required setup steps before these tasks, as well as [Running the Project](#running-the-project) for detailed runtime considerations.

## Requirements 
- An active Spotify account (can be free or paid)
- Python 3.9 or higher
- PostgreSQL 16 or higher
- Spotify Web API key
- **Primary Libraries Used:** Spotipy, SQLAlchemy, psycopg2, pandas

## Installation

### 1. Request Your Spotify Listening History
Request your Extended Streaming History from the Account/Privacy page of your Spotify account ([U.S. link](https://www.spotify.com/us/account/privacy/)). This will take up to 30 days to arrive, and you'll unfortunately have to wait for that before you can continue

### 2. Clone the repository
Using git bash...
```git clone https://github.com/christophercarney2/Spotify_Listening_History_Database```

### 3. Install the required Python packages
```pip install -r requirements.txt```

### 4. Set Up PostgreSQL
1. Install PostgreSQL on your machine if you haven't already. You can download it from the [official PostgreSQL website](https://www.postgresql.org/download/).

2. Create a new PostgreSQL database. The script expects you will use the default username (postgres), host (localhost), and port (5432). You can use the command line below or a GUI like PGAdmin to create the database.
	```psql -U postgres```
	```CREATE DATABASE spotify;```

3. Create an `.env` file with your PostgreSQL password stored as POSTGRESQL_PW.
	All additional database setup will be handled by the scripts.

### 5. Obtain a Spotify Web API Key 
1. Go to the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard/login).
    
2. Log in with your Spotify account or create a new account.
    
3. Click on "Create an App" and fill in the required details. Redirect URIs can be any site, for example https://google.com works just fine.
    
4. Once your app is created, you'll find your Client ID and Client Secret in the app settings.
    
5. Update the `.env` file with your Spotify Client ID (SPOTIFY_KEY) and Client Secret (SPOTIFY_SECRET)

### 6. Create config.ini File
Create a `config.ini` file with paths specific to your environment. Here's an example of what it should look like:

\[Paths\]
**project_root_path** = /path/to/your/project
data_path = **project_root_path**/MyData
artist_image_path = **project_root_path**/MyData/artist_images/ 
album_image_path = **project_root_path**/MyData/album_images/

Replace the values on the right side of the equals signs, using the value for **project_root_path** again for each of the subsequent paths.

Add this file to .gitignore to keep sensitive information from being posted to github.

### 7. Create logs directory
Create an empty logs directory in your **project_root_path** to store logs.

## Running the Project
This project is broken out into five separate scripts to be run individually by the user. This is because the Spotify Web API's rate limiting is opaque and inconsistent, making a manual restart of certain steps the most feasible solution. It also allows the user to ignore the final csv export step if they only plan to work with the data in PostgreSQL.

Before running scripts, ensure that your PostgreSQL server is running. Then make sure the listening history files returned by Spotify are present at your **data_path** (see above config.ini section) and named endsong_#.json, where the # increments by one from 0 for each subsequent file. Spotify has changed the names of these files over the course of development, so this cannot reasonably be automated.

**Run Order**
1. **setup_database.py**
	Creates the required tables in PostgreSQL. Will skip if already present.
2. **combine_and_load_listening_history.py**
	Loads to music_listening_history table by default.
3. **spotify_api_batch_processing.py**
	Will ask for an input of the Start Index. This is to avoid iterating over tracks that have already been checked after a rate limit. Spotify is not clear about what level of requests will trigger rate limiting, and my testing has proven inconsistent. Whenever a Max Retries error is returned, wait twelve hours or so and kick the script off again using the start index provided in the error log.
4. **db_updates.py**
	Normalizes IDs across tables and handles duplicate artists.
5. **download_images.py**
	Retrieves frequently occurring artist and album images from Spotify Web API.

6. **export_files_for_tableau.py**
	Exports the database tables as csvs, which is required if the user plans to use the data with Tableau Public, which does not allow for direct database data connections.

