import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
artist varchar,
auth varchar,
firstname varchar,
gender varchar,
iteminSession INTEGER,
LastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page varchar,
registration varchar,
sessionid integer,
song varchar,
status INTEGER,
ts INTEGER,
userAgent varchar,
UserId integer
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events_table_create (

song_id VARCHAR,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INTEGER);

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table_create (
songplay_id INTEGER PRIMARY KEY,
Start_time timestamp not null references dim_time(start_time),
user_id INTEGER not null references dim_user(user_id),
level varchar,
song_id varchar not null references dim_song(song_id),
artist_id varchar not null references dim_artist(artist_id),
session_id integer,
location varchar,
user_agent varchar,
user_id INTEGER);

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user 
(
            user_id    INTEGER PRIMARY KEY distkey, 
            first_name VARCHAR NOT NULL, 
            last_name  VARCHAR NOT NULL, 
            gender     VARCHAR NOT NULL, 
            level      VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song 
(
            song_id   VARCHAR PRIMARY KEY sortkey, 
            title     VARCHAR  NOT NULL, 
            artist_id VARCHAR NOT NULL, 
            year      INTEGER NOT NULL, 
            duration  FLOAT NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist 
(
            artist_id VARCHAR PRIMARY KEY distkey, 
            name      VARCHAR, 
            location  VARCHAR, 
            latitude  FLOAT NOT NULL, 
            longitude FLOAT NOT NULL
);
""")


time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
            start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
            hour       INTEGER, 
            day        INTEGER, 
            week       INTEGER, 
            month      INTEGER, 
            year       INTEGER, 
            weekday    INTEGER
);
""")

# STAGING TABLES

staging_events_copy = """
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS json {}
""".format(config.get('S3',  's3://udacity-dend/log_data'),
           config.get('IAM_ROLE','arn:aws:iam::459677937893:role/dwhRole'),
           config.get('S3', 's3://udacity-dend/log_json_path.json')
          )

staging_songs_copy = """
COPY staging_songs 
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS json 'auto'
""".format(config.get('S3','s3://udacity-dend/song_data'), 
           config.get('IAM_ROLE', 'arn:aws:iam::459677937893:role/dwhRole')
          )


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (
start_time,
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent)

SELECT DISTINCT to_timestamp(event.ts, 'YYYY-MM-DD HH24:MI:SS') as start_time,
                event.userId    AS user_id, 
                event.level     AS level, 
                song.song_id   AS song_id, 
                song.artist_id AS artist_id, 
                event.sessionId AS session_id, 
                event.location  AS location, 
                event.userAgent AS user_agent 
FROM staging_song as song
JOIN staging_events as event
ON (song.title = event.song)
AND event.page= 'Nextsong';
""")

user_table_insert = ("""
INSERT INTO user_table(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid as user_id,
firstname as first_name,
lastname as last_name,
gender,
level
FROM staging_events
WHERE userid <> NULL OR userid <> ''
AND page ='Nextsong';
""")

song_table_insert = ("""
INSERT INTO dim_song (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id AS song_id,
                title AS title, 
                artist_id AS artist_id, 
                year AS year, 
                duration AS duration
FROM  staging_songs
WHERE song_id IS NOT NULL;   
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id AS artist_id, 
                artist_name AS name, 
                artist_location AS location,  
                artist_latitude AS latitude, 
                artist_longitude AS longitude
FROM  staging_songs
WHERE artist_id IS NOT NULL; 
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TO_TIMESTAMP(ts,'YYYY-MM-DD HH24:MI:SS') AS start_time,
                EXTRACT(HOUR    FROM ts) AS hour,
                EXTRACT(DAY     FROM ts) AS day, 
                EXTRACT(WEEK    FROM ts) AS week, 
                EXTRACT(MONTH   FROM ts) AS month, 
                EXTRACT(YEAR    FROM ts) AS year,
                EXTRACT(WEEKDAY FROM ts) AS  weekday
FROM staging_events 
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
