import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session
    """
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads song_data to Spark cluster from S3. Creates the dimension tables songs and artists and store on S3.

    Parameters
    ----------
    spark : object
        A SparkSession object.
    input_data : str
        Input data bucket (on S3).
    output_data: str
        Output data bucket (on S3).
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    print("Loading song_data...")
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    columns = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.select(columns).distinct()

    print("Writing songs data...")
    # write songs table to parquet files partitioned by year and artist
    output_folder = output_data + "songs/songs.parquet"
    songs_table.write.parquet(
        output_folder,
        partitionBy=["year", "artist_id"],
        mode="overwrite",
    )

    # extract columns to create artists table
    columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.select(columns).distinct()

    print("Writing artists data...")
    # write artists table to parquet files
    output_folder = output_data + "artists/artists.parquet"
    artists_table.write.parquet(
        output_folder,
        mode="overwrite",
    )


def process_log_data(spark, input_data, output_data):
    """
    Loads log_data to Spark cluster from S3. Creates the dimension tables time and users and store on S3.
    Also creates the fact table songplays.

    Parameters
    ----------
    spark : object
        A SparkSession object.
    input_data : str
        Input data bucket (on S3).
    output_data: str
        Output data bucket (on S3).
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    print("Loding log data...")
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df.createOrReplaceTempView("log_data")
    print("Filtering log_data...")
    log_df = spark.sql(
        """
        SELECT * FROM log_data
        WHERE page = 'NextSong'
        """
    )

    # extract columns for users table
    columns = ["userId", "firstName", "lastName", "gender", "level"]
    users_table = log_df.select(columns).distinct()

    # write users table to parquet files
    output_folder = output_data + "users/users.parquet"
    print("Wrinting users table...")
    users_table.write.parquet(
        output_folder,
        mode="overwrite",
    )

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t: int(t) / 1000)
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda dt: datetime.fromtimestamp(dt))
    log_df = log_df.withColumn('datetime', get_datetime(log_df.timestamp))

    print("Calculating time table...")
    # extract columns to create time table
    time_table = log_df.select(
        log_df.datetime.alias("start_time"),
        hour(log_df.datetime).alias("hour"),
        dayofmonth(log_df.datetime).alias("dayofmonth"),
        weekofyear(log_df.datetime).alias("weekofyear"),
        month(log_df.datetime).alias("month"),
        year(log_df.datetime).alias("year"),
        dayofweek(log_df.datetime).alias("dayofweek"),
    ).distinct()

    # write time table to parquet files partitioned by year and month
    output_folder = output_data + "time/time.parquet"
    print("Wrinting time table...")
    time_table.write.parquet(
        output_folder,
        partitionBy=["year"],
        mode="overwrite",
    )

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    print("Loading song data...")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    song_df.createOrReplaceTempView("song_data")
    log_df.createOrReplaceTempView("log_data")
    print("Join data...")
    songplays_table = spark.sql(
        """
        SELECT
            "start_time",
            "userId",
            "level",
            "song_id",
            "artist_id",
            "sessionId",
            "location",
            "userAgent"
        FROM song_data
        INNER JOIN log_data
            ON song_data.artist_name = log_data.artist
        """
    )

    # write songplays table to parquet files partitioned by year and month
    output_folder = output_data + "songplays/songplays.parquet"
    print("Wrinting songplays table...")
    songplays_table.withColumn('year', year('start_time')).withColumn('month', month('start_time')).write.parquet(
        output_folder,
        partitionBy=["year", "month"],
        mode="overwrite",
    )


def main():
    """
    Extracts data from S3, compute on a Spark cluster em save on S3 (Star Schema).
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://pk-udacity-sparkify/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
