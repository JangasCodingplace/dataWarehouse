# Data Warehouse
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Datasets
The needed data is stored in Amazon S3.
* Song Data: `s3://udacity-dend/song_data`
* Log Data: `s3://udacity-dend/log_data`

## Database Schema
Following are the fact and dimension tables made for this project:

#### Dimension Tables:

**Table:** users
* columns: user_id, first_name, last_name, gender, level

**Table: songs**
* columns: song_id, title, artist_id, year, duration

**Table: artists**
* columns: artist_id, name, location, lattitude, longitude

**Table time**
* columns: start_time, hour, day, week, month, year, weekday

#### Fact Table:
**Table: songplays**
* columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


## Requirements
* `Python Version >= 3.6` (f-strings are in used)
* `psycopg2` (pip package)
* `sql_queries` (pip package)
* Redshift - You need valid Redshift credentials


## Run Project
* Create `dwh.cfg` File by using `cp dwh.cfg dwh.cfg.example`
* Fill `dwh.cfg` File
* Create Table by running `create_tables.py`
* Start Pipeline by running `etl.py`
