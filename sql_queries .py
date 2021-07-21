import configparser


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

staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events(\
                                artist VARCHAR,\
                                auth VARCHAR,\
                                first_name VARCHAR,\
                                gender VARCHAR,\
                                item_in_session INT,\
                                last_name VARCHAR,\
                                lenght FLOAT, \
                                level VARCHAR,\
                                location VARCHAR,\
                                method VARCHAR, \
                                page VARCHAR,\
                                registration FLOAT,\
                                session_id INT,\
                                song VARCHAR,\
                                status INT, \
                                ts BIGINT,\
                                user_agent VARCHAR, \
                                user_id INT)")


staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs(\
                                num_songs INT,\
                                artist_id VARCHAR,\
                                artist_latitude VARCHAR, \
                                artist_longitude VARCHAR, \
                                artist_location VARCHAR, \
                                artist_name VARCHAR, \
                                song_id VARCHAR, \
                                title VARCHAR, \
                                duration FLOAT, \
                                year INT)")




songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays(\
                            songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY,\
                            start_time TIMESTAMP ,\
                            user_id INT ,\
                            level VARCHAR,\
                            song_id VARCHAR  ,\
                            artist_id VARCHAR  ,\
                            session_id INT NOT NULL,\
                            location VARCHAR,\
                            user_agent VARCHAR)")

user_table_create = ("CREATE TABLE IF NOT EXISTS users(\
                       user_id INT PRIMARY KEY DISTKEY,\
                       first_name VARCHAR ,\
                       last_name VARCHAR ,\
                       gender VARCHAR ,\
                       level VARCHAR)")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs (\
                        song_id VARCHAR PRIMARY KEY DISTKEY,\
                        title VARCHAR,\
                        artist_id VARCHAR,\
                         year INT,\
                         duration FLOAT)")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists (\
                        artist_id VARCHAR PRIMARY KEY DISTKEY,\
                        name VARCHAR,\
                        location VARCHAR,\
                        latitude VARCHAR,\
                        longitude VARCHAR)")

time_table_create = ("CREATE TABLE IF NOT EXISTS time (\
                      start_time TIMESTAMP PRIMARY KEY SORTKEY,\
                      hour INT ,\
                      day INT ,\
                      week INT ,\
                      month INT ,\
                      year INT ,\
                      weekday INT)")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON '{}';
    """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
    """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("INSERT INTO songplays (start_time, user_id,\
                            level, song_id, artist_id, session_id, location, user_agent)\
                            SELECT DISTINCT  \
                                       timestamp 'epoch' + staging_events.ts * interval '1 second' AS start_time, \
                                       staging_events.user_id,\
                                       staging_events.level,\
                                       staging_songs.song_id,\
                                       staging_songs.artist_id, \
                                       staging_events.session_id, \
                                       staging_events.location,\
                                       staging_events.user_agent\
                            FROM  staging_events\
                            JOIN staging_songs ON (staging_songs.title = staging_events.song AND staging_songs.artist_name = staging_events.artist)\
                            WHERE staging_events.page = 'NextSong'")

user_table_insert = ("INSERT INTO users (user_id, first_name, last_name, gender, level)\
                         SELECT DISTINCT staging_events.user_id, \
                                staging_events.first_name,\
                                staging_events.last_name,\
                                staging_events.gender,\
                                staging_events.level\
                        FROM staging_events\
                        WHERE staging_events.user_id IS NOT NULL")

song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration)\
                        SELECT DISTINCT staging_songs.song_id , \
                               staging_songs.title,\
                               staging_songs.artist_id,\
                               staging_songs.year,\
                               staging_songs.duration\
                        FROM staging_songs\
                        WHERE staging_songs.song_id IS NOT NULL")

artist_table_insert = ("INSERT INTO artists (artist_id, name, location, latitude, longitude)\
                        SELECT DISTINCT staging_songs.artist_id , \
                               staging_songs.artist_name AS name,\
                               staging_songs.artist_location AS location,\
                               staging_songs.artist_latitude AS latitude,\
                               staging_songs.artist_longitude AS longitude\
                        FROM staging_songs\
                        WHERE staging_songs.artist_id IS NOT NULL")


time_table_insert = ("INSERT INTO time (start_time, hour, day, week, month, year, weekday)\
                        SELECT DISTINCT songplays.start_time,\
                                EXTRACT (hour FROM  songplays.start_time),\
                                EXTRACT (day FROM  songplays.start_time),\
                                EXTRACT (week FROM  songplays.start_time),\
                                EXTRACT (month FROM  songplays.start_time),\
                                EXTRACT (year FROM  songplays.start_time),\
                                EXTRACT (weekday FROM  songplays.start_time)\
                        FROM songplays\
                        WHERE songplays.start_time IS NOT NULL")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [ staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop] 
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
