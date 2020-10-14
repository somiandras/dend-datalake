import configparser
from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession

from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """Creates Spark session to be used later

    Returns:
        SparkSession
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_data(spark, input_data, output_data):
    """Process read JSON data from S3, transform and write back to S3 in
    partitioned Parquet files.

    Args:
        spark (SparkSession): Spark session to use
        input_data (string): Path of input data on S3
        output_data (string): Path of output data on S3
    """
    log = spark.read.json(input_data + "log_data/2018/11/*").filter(
        'page == "NextSong"'
    )
    songs = spark.read.json(input_data + "song_data/*/*/*")

    join_cond = [
        log.artist == songs.artist_name,
        log.song == songs.title,
        log.length == songs.duration,
    ]

    songplay = (
        log.join(songs, join_cond, "left")
        .withColumn("songplay_id", F.monotonically_increasing_id())
        .withColumnRenamed("ts", "start_time")
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("sessionId", "session_id")
        .withColumnRenamed("userAgent", "user_agent")
        .select(
            "songplay_id",
            "start_time",
            "user_id",
            "level",
            "song_id",
            "artist_id",
            "session_id",
            "location",
            "user_agent",
        )
    )

    user = (
        log.groupby("userId")
        .agg(F.max("ts").alias("ts"))
        .join(log, ["userId", "ts"])
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
        .select("user_id", "first_name", "last_name", "gender", "level")
    )

    song = (
        songplay.select("song_id")
        .distinct()
        .join(songs, "song_id")
        .select("song_id", "title", "artist_id", "year", "duration")
    )

    artist = (
        songplay.select("artist_id")
        .distinct()
        .join(songs.dropDuplicates(subset=["artist_id"]), "artist_id")
        .withColumnRenamed("artist_name", "name")
        .withColumnRenamed("artist_location", "location")
        .withColumnRenamed("artist_latitude", "latitude")
        .withColumnRenamed("artist_longitude", "longitude")
        .select("artist_id", "name", "location", "latitude", "longitude")
    )

    time = (
        log.withColumn("timestamp", F.to_timestamp(log["ts"] / 1000))
        .dropDuplicates(subset=["ts"])
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("day", F.dayofmonth("timestamp"))
        .withColumn("week", F.weekofyear("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("dow", F.dayofweek("timestamp"))
        .withColumnRenamed("ts", "start_time")
        .select("start_time", "hour", "day", "week", "month", "year", "dow")
    )

    # Write out results
    (
        songplay.withColumn("timestamp", F.to_timestamp(songplay["start_time"] / 1000))
        .withColumn("year", F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .drop("timestamp")
        .write.parquet(output_data + "songplay", partitionBy=["year", "month"])
    )

    user.write.parquet(output_data + "user")
    song.write.parquet(output_data + "song", partitionBy=["year", "artist_id"])
    artist.write.parquet(output_data + "artist")
    time.write.parquet(output_data + "time", partitionBy=["year", "month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-projects-somi/spark/"

    process_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
