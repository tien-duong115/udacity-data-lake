import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, udf, col, to_timestamp, monotonically_increasing_id, abs
from pyspark.sql.types import TimestampType as Stamp
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Create Spark sessions with this function"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate() 
    return spark


def process_song_data(spark, input_data, output_data):
    """Take in location of bucket to process song_data and return output to another bucket
    
    - spark --> spark session cursor
    
    - input_data --> s3 bucket connection end point with json file of song_data 
    
    - output_data --> s3 bucket connection end point for storing song_table and artist_table 
    """
    # get filepath to song data file
    input
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'songs/song.parquet'), 'overwrite')
    

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')\
                    .withColumnRenamed('artist_name', 'name') \
                    .withColumnRenamed('artist_location', 'location') \
                    .withColumnRenamed('artist_latitude', 'latitude') \
                    .withColumnRenamed('artist_longitude', 'longitude') \
                    .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artist/artist.parquet'), 'overwrite')

def process_log_data(spark, input_data, output_data):
    """
    Load in the song and log dataset from json file,
    extract the columns requirement for songplay table by joining song and log data,
    write into parquet file with partition and load in S3 BUCKET
    
    - spark --> Spark session cursor
    
    - Input_data --> path for log file in s3 bucket
    
    - Output_data --> path to load log data into s3 bucket
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = df.where('page="NextSong"')

    # extract columns for users table    
    user_table = log_df.select('userid', 'firstName', 'lastName', 'gender', 'level')
    user_table = user_table.dropDuplicates(["userid"])

    # write users table to parquet files
    user_table.write.partitionBy('userid').parquet(os.path.join(output_data, 'users/parquet.users'), 'overwrite')
    

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)), Stamp())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)), Stamp())
    dfs = df.withColumn("datetime", get_timestamp(col("ts")))

    time_table = dfs.select(
                        F.col("timestamp").alias("start_time"),
                        F.hour("timestamp").alias('hour'),
                        F.dayofmonth("timestamp").alias('day'),
                        F.weekofyear("timestamp").alias('week'),
                        F.month("timestamp").alias('month'), 
                        F.year("timestamp").alias('year'), 
                        F.date_format(F.col("timestamp"), "E").alias("weekday")
                    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month')\
        .parquet(os.path.join(output_data, 'time/parquet.time'), 'overwrite')
    
    # read in song data to use for songplays table
    song_data =  os.path.join(input_data,'song_data/A/A/A/*.json')

    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    #Create a temp table join together log_df and song_df at title, artist and duration
    song_df.createOrReplaceTempView("song_data")
    dfs.createOrReplaceTempView("log_data")
    songplay = spark.sql("""
                                    SELECT monotonically_increasing_id() as songplay_id,
                                    log.timestamp as start_time,
                                    year(log.timestamp) as year,
                                    month(log.timestamp) as month,
                                    log.userId as user_id,
                                    log.level as level,
                                    song.song_id as song_id,
                                    song.artist_id as artist_id,
                                    log.sessionId as session_id,
                                    log.location as location,
                                    log.userAgent as user_agent
                                    FROM log_data log
                                    JOIN song_data song
                                    ON (log.song = song.title
                                    AND log.length = song.duration
                                    AND log.artist = song.artist_name)
                                    """)
    
    # write songplays table to parquet files partitioned by year and month
    songplay.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("songplays.parquet completed")
    print("process_log_data completed")
    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://tien-bucket/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
