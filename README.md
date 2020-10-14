# Udacity DEND - Spark Project

## Purpose

The goal of the current project is to create fact and dimension tables to support user behavior analyis. The source data resides in S3 in the form of raw JSON files. The script needs to write the resulting table back to S3, but in partitioned Parquet files.

## Schema

I used very similar schema than in the previous project, but this time created by using the Spark dataframe API. THe resulting schema is a star schema where `songplay` table functions as the central fact table. `user`, `artist`, `song` and `time` tables serve as dimension tables, each holding columns relevant to these entities. The tables are stored under at `s3a://dend-projects-somi/spark/`.

### Fact Table: `songplays`

Records in log data associated with song plays i.e. records with page NextSong: _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

### Dimension Tables

#### `users`

Users in the app with _user_id, first_name, last_name, gender, level_.

I selected the last songplay action for each user and saved the user details from these records, to have the latest `level` value for each users in the final tables.

#### `songs`

Songs in music database: _song_id, title, artist_id, year, duration_

I only included those songs in the final table that were actually played according to log data. This is important because the song data contains more than 14K songs, most of which would be useless in our dimension table.

#### `artists`

Artists in music database: _artist_id, name, location, lattitude, longitude_.

Due to inconsistencies in artist names (same `artist_id` can haven slightly different `artist_name` for different songs), I chose the last occurence of each `artist_id` in the log data, and saved the artist details found on these events.

#### `time`

Timestamps of records in songplays broken down into specific units:  _start_time, hour, day, week, month, year, weekday_

## ETL process

I created a single ETL function to be able to use both data sources - log data and song data - when creating `songplay` table with avoiding the overhead of reading the data multiple times.

The ETL process uses the builtin methods of Spark DataFrame API. I avoided creating UDF for transforming `ts` values to proper timestamps and extracting the components. I used `pyspark.sql.functions.to_timestamp()` instead on `ts` column, and then called relevant date related functions from `pyspark.sql.functions` (eg. `hour`, `dayofmonth`, etc.).

Writing out the resulting dataframes to Parquet files is straightforward using the `write.parquet()` method of the DataFrames.
