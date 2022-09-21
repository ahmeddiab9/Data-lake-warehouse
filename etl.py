import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, TimestampType, FloatType, DoubleType

# Read Aws Configrtion File 
config = configparser.ConfigParser()
config.read('dl.cfg')
# Read AWS ACCESS KEY
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
# Read AWS SECRET KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """_summary_    
        Create SparkSession
    Returns:
        Object: Spark Object
    """
    spark = SparkSession \
        .builder \
        .appName("ETL Project")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """_summary_
        Proccess Song Data 
        Read Data From S3 
        Select Specific Columns For Table (songs,artists)
        Load Data To S3 Data Lake 
    Args:
        spark (Object):       SparkSession Object To Work With Spark
        input_data (String):  Input Data Path
        output_data (String): OutPut Data Path
    """
    # get filepath to song data file
    print("Create Song Data Path...")
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')

    # Create Schema For Data
    schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", FloatType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", FloatType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])
    # read song data file
    print("Read Song Data AS DataFrame With New Schema....")
    df = spark.read.json(song_data, schema=schema)

    # extract columns to create songs table
    print("Extract Column For Songs Table....")
    songs_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(songs_columns).where(
        df.song_id.isNotNull()).dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    print("Write Songs Data As parquet File To Songs Folder....")
    songs_table.write.partitionBy(
        ['year', 'artist_id']).parquet(output_data + "songs/")

    # extract columns to create artists table
    print("Extract Data For Artists....")
    artists_table = (df
                     .select(['artist_id',
                              expr('artist_name  as name'),
                              expr("artist_location as location"),
                              expr("artist_latitude as lattitude"),
                              expr("artist_longitude as longitude")])
                     .where(df.artist_id.isNotNull())
                     .dropDuplicates(['artist_id']))

    # write artists table to parquet files
    print("Write artists Data As parquet File To artists Folder....")
    artists_table.write.parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    """_summary_
        Procccess Log Data
        Read Data From S3 
        Select Specific Columns For Table (users,time,songplays)
        Load Data To S3 Data Lake 
    Args:
        spark (Object):       SparkSession Object To Work With Spark
        input_data (String):  Input Data Path
        output_data (String): OutPut Data Path
    """
    # get filepath to log data file
    print("Create Log Data Path...")
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    print("Read Log Data AS DataFrame With....")
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    print("Extract Column For users Table....")
    users_columns = ["userId as user_id", "firstName as first_name",
                     "lastName as last_name", "gender", "level"]
    artists_table = (df
                     .selectExpr(users_columns)
                     .where(df.userId.isNotNull())
                     .dropDuplicates(['user_id']))

    # write users table to parquet files
    print("Write users Data As parquet File To users Folder....")
    artists_table.write.parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x / 1000.0), IntegerType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    df = df.withColumn('start_time', from_unixtime(df.timestamp))

    # extract columns to create time table
    print("Extract Column For time Table....")
    time_table = (df
                  .select("start_time")
                  .withColumn("hour", hour("start_time"))
                  .withColumn("day", dayofmonth("start_time"))
                  .withColumn("week", weekofyear("start_time"))
                  .withColumn("month", month("start_time"))
                  .withColumn("year", year("start_time"))
                  .withColumn("weekday", date_format("start_time", "E")))

    # write time table to parquet files partitioned by year and month
    print("Write time Data As parquet File To time Folder....")
    time_table.write.partitionBy(
        'year', 'month').parquet(output_data + "time/")

    # read in song data to use for songplays table
    print("Read Songs Data From S3.... ")
    song_df_path = os.path.join(input_data, 'song_data/A/A/*/*.json')
    song_df = spark.read.json(song_df_path)
    # extract columns from joined song and log datasets to create songplays table
    print("Extract Column For songplays Table....")
    # Join Condition
    cond = [df.song == song_df.title, df.artist == song_df.artist_name, df.length == song_df.duration]
    songplays_table = df.join(song_df, cond, how='left')
    songplays_columns = ["start_time",
                         "userId as user_id", "level", "song_id",
                         "artist_id", "sessionId as session_id",
                         "artist_location as location", "userAgent as user_agent"]

    songplays_table = (songplays_table.selectExpr(songplays_columns)
                       .withColumn("month", month("start_time"))
                       .withColumn("year", year("start_time")))

    # write songplays table to parquet files partitioned by year and month
    print("Write time Data As parquet File To songplays Folder....")
    songplays_table.write.partitionBy(
        ["year", "month"]).parquet(output_data + "songplays/")


def main():
    # Create Spark Session
    print("Create SparkSession..")
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://project1993/output_data/"

    print("Run Proccess Song Data....")
    process_song_data(spark, input_data, output_data)
    print("Finish Song Data And Loaded ):")
    print("Run Proccess Log Data....")
    process_log_data(spark, input_data, output_data)
    print("Finish Log Data And Loaded... ):")
    spark.stop()
    print("Done ETL successfully ):")


if __name__ == "__main__":
    main()
