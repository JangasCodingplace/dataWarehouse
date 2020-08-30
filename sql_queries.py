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

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length double precision,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration double precision,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        user_id varchar
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id varchar,
        artist_latitude varchar,
        artist_location varchar,
        artist_longitude varchar,
        artist_name varchar,
        duration double precision,
        num_songs int,
        song_id varchar,
        title varchar,
        year smallint
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int NOT NULL IDENTITY(0,1),
        start_time timestamp NOT NULL,
        user_id varchar NOT NULL,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar,
        PRIMARY KEY (songplay_id)
    )
    DISTKEY (song_id)
    SORTKEY(start_time, session_id);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id varchar NOT NULL,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar NOT NULL,
        PRIMARY KEY (user_id)
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar NOT NULL,
        title varchar,
        artist_id varchar NOT NULL,
        year int,
        duration double precision,
        PRIMARY KEY (song_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    )
    DISTKEY (song_id)
    SORTKEY (year);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar NOT NULL,
        name varchar NOT NULL,
        location varchar,
        latitude varchar,
        longitude varchar,
        PRIMARY KEY (artist_id)
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time bigint NOT NULL,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int,
        PRIMARY KEY (start_time)
    )
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events
    FROM {config.get('S3', 'LOG_DATA')}
    CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE','ARN')}'
    FORMAT AS JSON {config.get('S3','LOG_JSONPATH')}
    REGION 'us-west-2';
""")


staging_songs_copy = (f"""
    COPY staging_songs
    FROM {config.get('S3', 'SONG_DATA')}
    CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE','ARN')}'
    FORMAT AS JSON 'auto' REGION 'us-west-2' TRUNCATECOLUMNS;
""")

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT timestamp 'epoch' + ts * interval '0.001 seconds' as start_time,
           user_id,
           level,
           songs.song_id as song_id,
           artists.artist_id as artist_id,
           sessionId as session_id,
           staging_events.location as location,
           userAgent as user_agent
    from staging_events
    inner join artists on artists.name = staging_events.artist
    inner join songs on songs.title = staging_events.song
    where page='NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT user_id, firstName, lastName, gender, level
    FROM staging_events
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        year,
        weekday
    )
    SELECT DISTINCT
        start_time,
        extract(hour from timestamp 'epoch' + start_time * interval '0.001 seconds') as hour,
        extract(day from timestamp 'epoch' + start_time * interval '0.001 seconds') as day,
        extract(week from timestamp 'epoch' + start_time * interval '0.001 seconds') as week,
        extract(year from timestamp 'epoch' + start_time * interval '0.001 seconds') as year,
        extract(weekday from timestamp 'epoch' + start_time * interval '0.001 seconds') as weekday
    FROM songplays
""")


# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    songplay_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries=[
    song_table_insert,
    artist_table_insert,
    user_table_insert,
    songplay_table_insert,
    time_table_insert
]
