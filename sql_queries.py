import configparser

#Fact: songplays - records in event data associated with song plays i.e. records with page NextSong
#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#Dim: users - users in the app 
#user_id, first_name, last_name, gender, level

#Dim: songs - songs in music database
#song_id, title, artist_id, year, duration

#Dim: artists - artists in music database
#artist_id, name, location, lattitude, longitude

#Dim: time - timestamps of records in songplays broken down into specific units
#start_time, hour, day, week, month, year, weekday

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
  artist           VARCHAR(255),
  auth             VARCHAR(255) NOT NULL,
  firstName        VARCHAR(255),
  gender           VARCHAR(1),
  itemInSession    BIGINT NOT NULL,
  lastName         VARCHAR(255),
  length           FLOAT,
  level            VARCHAR(255) NOT NULL,
  location         VARCHAR(255),
  method           VARCHAR(255) NOT NULL,
  page             VARCHAR(255) NOT NULL,
  registration     FLOAT,
  sessionId        BIGINT NOT NULL,
  song             VARCHAR(255),
  status           BIGINT NOT NULL,
  ts               BIGINT NOT NULL,
  userAgent        VARCHAR(4096),
  userId           BIGINT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
  num_songs        BIGINT NOT NULL,
  artist_id        VARCHAR(255) NOT NULL,
  artist_latitude  FLOAT,
  artist_longitude FLOAT,
  artist_location  VARCHAR(255),
  artist_name      VARCHAR(255) NOT NULL,
  song_id          VARCHAR(255) NOT NULL,
  title            VARCHAR(255) NOT NULL,
  duration         FLOAT NOT NULL,
  year             BIGINT
);
""")

songplay_table_create = ("""
CREATE TABLE songplays
(
  songplay_id      BIGINT IDENTITY(0,1) sortkey,
  start_time       BIGINT NOT NULL,
  user_id          BIGINT NOT NULL,
  level            VARCHAR(32) NOT NULL,
  song_id          VARCHAR(64) distkey,
  artist_id        VARCHAR(64),
  session_id       BIGINT NOT NULL,
  location         VARCHAR(255),
  user_agent       VARCHAR(4096) NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users
(
  user_id          BIGINT NOT NULL sortkey,
  first_name       VARCHAR(128),
  last_name        VARCHAR(128),
  gender           VARCHAR(1),
  level            VARCHAR(32)
);
""")

song_table_create = ("""
CREATE TABLE songs
(
  song_id          VARCHAR(64) NOT NULL sortkey distkey,
  title            VARCHAR(255) NOT NULL,
  artist_id        VARCHAR(64) NOT NULL,
  year             BIGINT,
  duration         FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE artists
(
  artist_id        VARCHAR(64) NOT NULL sortkey,
  name             VARCHAR(255) NOT NULL,
  location         VARCHAR(255),
  latitude         FLOAT,
  longitude        FLOAT
);
""")

time_table_create = ("""
CREATE TABLE time
(
  start_time       DATETIME NOT NULL sortkey,
  hour             BIGINT NOT NULL,
  day              BIGINT NOT NULL,
  week             BIGINT NOT NULL,
  month            BIGINT NOT NULL,
  year             BIGINT NOT NULL,
  weekday          BIGINT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
json {}
region {}
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'),config.get('REGION','REGION'))

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
json {}
region {}
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'),config.get('AUTO','AUTO'),config.get('REGION','REGION'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
e.ts,
e.userid,
e.level,
s.song_id,
s.artist_id,
e.sessionid,
e.location,
e.useragent AS user_agent
FROM staging_events e
LEFT JOIN staging_songs s ON e.artist = s.artist_name AND e.song = s.title
WHERE e.page = 'NextSong'
;
""")

user_table_insert = ("""
INSERT INTO users
(
SELECT DISTINCT
userid AS user_id,
firstname AS first_name,
lastname AS last_name,
gender,
level
FROM staging_events
WHERE userid IS NOT NULL
);
""")

song_table_insert = ("""
INSERT INTO songs
(
SELECT DISTINCT
song_id,
title,
artist_id,
CASE WHEN year = 0 OR year IS NULL THEN NULL ELSE year END AS year,
duration
FROM staging_songs
);
""")

artist_table_insert = ("""
INSERT INTO artists
(
SELECT DISTINCT
artist_id,
artist_name AS name,
artist_location AS location,
artist_latitude AS latitude,
artist_longitude AS longtitude
FROM staging_songs
);
""")

time_table_insert = ("""
INSERT INTO time
(
SELECT DISTINCT
(timestamp 'epoch' + ts/1000 * interval '1 second')::datetime AS start_time,
DATE_PART('hour', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS hour,
DATE_PART('day', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS day,
DATE_PART('week', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS week,
DATE_PART('month', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS week,
DATE_PART('year', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS year,
DATE_PART('weekday', timestamp 'epoch' + ts/1000 * interval '1 second')::int AS weekday
FROM staging_events
WHERE page = 'NextSong'
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
