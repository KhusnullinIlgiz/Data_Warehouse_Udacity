# Data Warehouse Udacity project

## Goal of this project 
The goal of this project is to create an ETL process to extract, transform and load data from existing JSON log and song files to AWS Redshift Warehouse with denormalised star schema. Denormalised star schema will allow to quick analysys of data by using simple queries without JOIN statements. Data Warehouse hast an ability to distribute data by key among CPU's which encreases query the Data. My personal goal in this project is to gain experience in building ETL pipelines with AWS Redshift.

# ETL Process

First staging tables created. They contain all raw data from JSON song and log files from S3 bucket. After that final denormalised tables created and data is inserted from staging tables.

# Tables structure and datatypes

> ### Staging tables
1. **staging_events** - all data from JSON log files
> - **artist** VARCHAR, **auth** VARCHAR, **first_name** VARCHAR, **gender** VARCHAR, **item_in_session** INT, **last_name** VARCHAR, **lenght** FLOAT, **level** VARCHAR, **location** VARCHAR, **method** VARCHAR, **page** VARCHAR, **registration** FLOAT, **session_id** INT, **song** VARCHAR, **status** INT, **ts** BIGINT, **user_agent** VARCHAR, **user_id** INT

2. **staging_songs** - all data from JSON song files

> - **num_songs** INT, **artist_id** INT, **artist_latitude** VARCHAR, **artist_longitude** VARCHAR, **artist_location** VARCHAR, **artist_name** VARCHAR, **song_id** VARCHAR, **title** VARCHAR, **duration** FLOAT, , **year** INT

> ### Final tables
3. **songplays** - records with page NextSong from staging tables

> - **songplay_id** BIGINT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY, **start_time BIGINT**, **user_id** INT, **level** VARCHAR, **song_id** VARCHAR, **artist_id** VARCHAR, **session_id** INT, **location** VARCHAR, **user_agent** VARCHAR

4. **users** - users in the app from staging tables

> - **user_id** INT PRIMARY KEY DISTKEY, **first_name** VARCHAR, **last_name** VARCHAR, **gender** VARCHAR, **level** VARCHAR

5. **songs** - songs from staging tables

> - **song_id** VARCHAR PRIMARY KEY DISTKEY, **title** VARCHAR, **artist_id** VARCHAR, **year** INT, **duration** FLOAT

6. **artists** - artists from staging tables

> - **artist_id** VARCHAR PRIMARY KEY DISTKEY, **name** VARCHAR, **location** VARCHAR, **latitude** DECIMAL(9,6), **longitude** DECIMAL(9,6)

7. **time** - timestamps of records in **songplays** broken down into specific units

> - **start_time** TIMESTAMP PRIMARY KEY SORTKEY, **hour** INT, **day** INT, **week** INT, **month** INT, **year** INT, **weekday** VARCHAR,


# Datasets

## Song Dataset

This dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).
Example of JSON file in data/songs directory:

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

## Log Dataset

This dataset represents activities of users which are then stored in JSON log files.
Example of JSON file in data/logs directory:

{"artist":null,"auth":"Logged In","firstName":"Adler","gender":"M","itemInSession":0,"lastName":"Barrera","length":null,"level":"free","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"GET","page":"Home","registration":1540835983796.0,"sessionId":248,"song":null,"status":200,"ts":1541470364796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.78.2 (KHTML, like Gecko) Version\/7.0.6 Safari\/537.78.2\"","userId":"100"}


# Query examples

## SELECT * FROM users LIMIT 1;

|user_id    |first_name|last_name |gender|level  |
| --------- | -------- | -------- | ---- | ----- |
|     39    | Walter   | Frye     | M    | Free  |


## SELECT * FROM songs LIMIT 1;

|artist_id                |name          |location  |latitude    |longitude            |
| --------------------- | -------------- | -------- | ---------- | ------------------- |
| ARBZIN01187FB362CC    | Paris Hilton   | 27       | 1.32026    | 103.78870999999999  |

# Project Files Structure Description

1. **sql_queries.py** - contains SQL (**CREATE TABLE**, **INSERT INTO**, **DROP TABLE**) statements for each Staging and Final tables

2. **create_tables.py** - executes queries from **sql_queries.py**, creates/connects to Redshift

3. **dwh.cfg** - contains credentials for Redshift and S3 buckets 

4. **etl.py** - extract transform and load data from logs and songs datasets to created tables in Redshift


## How to run

Run **create_tables.py** before running **etl.py** to reset your tables.

