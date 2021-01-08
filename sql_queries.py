import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE stg_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession varchar,
        lastName varchar,
        length varchar,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId varchar,
        song varchar,
        status varchar,
        ts varchar,
        userAgent varchar,
        userId varchar
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE stg_songs (
        num_songs varchar,
        artist_id varchar,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration varchar,
        year varchar
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level text NOT NULL,
        song_id text NOT NULL,
        artist_id text NOT NULL,
        session_id int,
        location text NOT NULL,
        user_agent text
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id int PRIMARY KEY,
        first_name text,
        last_name text,
        gender text,
        level text NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id text PRIMARY KEY,
        title text,
        artist_id text NOT NULL,
        year int,
        duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id text PRIMARY KEY,
        name text,
        location text,
        latitude text,
        longitude text
    )
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stg_events from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json '{}';
""").format(LOG_DATA, ARN, LOG_JSONPATH)
print(staging_events_copy)

staging_songs_copy = ("""
    copy stg_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second', se.userId::int, se.level, ss.song_id, ss.artist_id, se.sessionId::int, se.location, se.userAgent
    FROM stg_events as se
    JOIN stg_songs as ss
    ON se.artist = ss.artist_name AND se.song = ss.title AND se.length = ss.duration
    WHERE se.page='NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT userid::int, firstname, lastname, gender, level
    FROM stg_events
    WHERE page='NextSong'
    AND userid NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT song_id, title, artist_id, year::int, duration
    FROM stg_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM stg_songs;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), extract(month from start_time), extract(year from start_time), extract(weekday from start_time)
    FROM stg_events
    WHERE page='NextSong'
    AND ts is not NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
