CREATE TABLE music_listening_history (
	music_stream_id BIGSERIAL PRIMARY KEY,
	time_ended TIMESTAMP,
	ms_played BIGINT,
	track_name VARCHAR(1000),
	artist_name VARCHAR(1000),
	spotify_artist_id VARCHAR(22),
	spotify_album_id VARCHAR(22),
	album_name VARCHAR(1000),
	spotify_track_uri VARCHAR(36),
	reason_started VARCHAR(100),
	reason_ended VARCHAR(100),
	shuffle BOOLEAN,
	skipped BOOLEAN,
	incognito BOOLEAN);


CREATE TABLE artists (
	spotify_artist_id VARCHAR(37) PRIMARY KEY,
	artist_name VARCHAR(1000),
	artist_popularity INT,
	followers BIGINT,
	main_genre VARCHAR(100),
	genres TEXT);


CREATE TABLE albums (
	spotify_album_id VARCHAR PRIMARY KEY,
	album_name VARCHAR,
	spotify_artist_id VARCHAR,
	artist_name VARCHAR,
	album_type VARCHAR,
	total_tracks INTEGER,
	label VARCHAR,
	release_date DATE,
	release_date_precision VARCHAR
	);


CREATE TABLE artist_genre (
	spotify_artist_id VARCHAR,
	artist_name VARCHAR,
	genre VARCHAR
	);

CREATE TABLE tracks (
	spotify_track_uri VARCHAR,
	spotify_track_id VARCHAR PRIMARY KEY,
	track_name VARCHAR,
	spotify_artist_id VARCHAR,
	spotify_album_id VARCHAR,
	duration_ms INTEGER,
	track_popularity INT,
	acousticness FLOAT,
	danceability FLOAT,
	energy FLOAT,
	instrumentalness FLOAT,
	liveness FLOAT,
	loudness FLOAT,
	speechiness FLOAT,
	valence FLOAT,
	tempo FLOAT,
	key INT,
	time_signature INT
);


CREATE TABLE tracks_consolidated (
	spotify_track_uri VARCHAR,
	spotify_track_id VARCHAR PRIMARY KEY,
	track_name VARCHAR,
	spotify_artist_id VARCHAR,
	spotify_album_id VARCHAR,
	duration_ms INTEGER,
	track_popularity INT,
	acousticness FLOAT,
	danceability FLOAT,
	energy FLOAT,
	instrumentalness FLOAT,
	liveness FLOAT,
	loudness FLOAT,
	speechiness FLOAT,
	valence FLOAT,
	tempo FLOAT,
	key INT,
	time_signature INT
);


CREATE TABLE track_artists (
	spotify_track_uri VARCHAR,
	spotify_track_id VARCHAR,
	spotify_artist_id VARCHAR,
	PRIMARY KEY(spotify_track_id, spotify_artist_id);



--This will pull the linked tracks and combine them for a new tracks table

--Many tracks have multiple versions that are very close in length. This removes
--unnecessary duplicates where the track durations are within three seconds of each 
--other. Much longer and things like live versions start being disproportionately
--and unwarrantedly removed.

INSERT INTO tracks_consolidated (spotify_track_uri,
		   spotify_track_id,
		   track_name,
		   spotify_artist_id,
		   spotify_album_id,
		   duration_ms,
		   track_popularity,
		   acousticness,
		   danceability,
		   energy,
		   instrumentalness,
		   liveness,
		   loudness,
		   speechiness,
		   valence,
		   tempo,
		   key,
		   time_signature)


select 	    
		   spotify_track_uri,
		   spotify_track_id,
		   track_name,
		   spotify_artist_id,
		   spotify_album_id,
		   duration_ms,
		   track_popularity,
		   acousticness,
		   danceability,
		   energy,
		   instrumentalness,
		   liveness,
		   loudness,
		   speechiness,
		   valence,
		   tempo,
		   key,
		   time_signature from (
	select distinct t.spotify_track_uri,
		   t.spotify_track_id,
		   t.track_name,
		   t.spotify_artist_id,
		   t.spotify_album_id,
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
		   row_number() over (partition by t.spotify_artist_id, 
											t.track_name
							  order by case when a.album_type = 'album' then 1 when a.album_type = 'single' then 2 when a.album_type = 'compilation' then 3 else 4 end, 
													t.track_popularity desc, 
													a.release_date, 
													t.spotify_track_id) rnk
	from tracks t
	join tracks t2
		on t.track_name = t2.track_name
		and t.spotify_artist_id = t2.spotify_artist_id
join albums a
		on t.spotify_album_id = a.spotify_album_id
where t.duration_ms between (t2.duration_ms - 3000) and (t2.duration_ms + 3000)
	)
where rnk = 1
;

--This will create the XREF

CREATE TABLE IF NOT EXISTS track_mapping (
    old_track_uri TEXT PRIMARY KEY,
    new_track_uri TEXT
);


INSERT INTO track_mapping
select coalesce(y.spotify_track_uri,x.spotify_track_uri) old_track_uri,
	   x.spotify_track_uri as new_track_uri from
(select 	    
		   spotify_track_uri,
		   spotify_track_id,
		   track_name,
		   spotify_artist_id,
		   spotify_album_id,
		   duration_ms,
		   track_popularity,
		   acousticness,
		   danceability,
		   energy,
		   instrumentalness,
		   liveness,
		   loudness,
		   speechiness,
		   valence,
		   tempo,
		   key,
		   time_signature from (
	select distinct t.spotify_track_uri,
		   t.spotify_track_id,
		   t.track_name,
		   t.spotify_artist_id,
		   t.spotify_album_id,
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
		   row_number() over (partition by t.spotify_artist_id, 
											t.track_name
							  order by case when a.album_type = 'album' then 1 when a.album_type = 'single' then 2 when a.album_type = 'compilation' then 3 else 4 end, 
													t.track_popularity desc, 
													a.release_date, 
													t.spotify_track_id) rnk
	from tracks t
	join tracks t2
		on t.track_name = t2.track_name
		and t.spotify_artist_id = t2.spotify_artist_id
join albums a
		on t.spotify_album_id = a.spotify_album_id
where t.duration_ms between (t2.duration_ms - 3000) and (t2.duration_ms + 3000)
	)
where rnk = 1
) x
left outer join
tracks y
on x.spotify_artist_id = y.spotify_artist_id
and x.track_name = y.track_name
and x.duration_ms = y.duration_ms;




--This will update the genres field in artists

UPDATE artists AS a
SET genres = ag.genres
FROM (
    SELECT spotify_artist_id, string_agg(genre, ', ') AS genres
    FROM artist_genre
    GROUP BY spotify_artist_id
) AS ag
WHERE a.spotify_artist_id = ag.spotify_artist_id;