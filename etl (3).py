import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


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
    song_data = 's3://udacity-dend/song_data'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title",
                           "artist_id", "year", "duration") \
                            .dropDuplicates()
    song_table.createOrReplaceTempview('song')
    
    # write songs table to parquet files partitioned by year and artist
    song_table = song_table.partitionBy("year", "artist_id").partquet(os.path.join(output_data, 'songs/song.parquet'), 'overwrite')
    

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longtitude')\
                    .withColumnRenamed('artist_name', 'name') \
                    .withColumnRenamed('artist_location', 'location') \
                    .withColumnRenamed('artist_latitude', 'latitude') \
                    .withColumnRenamed('artist_longitude', 'longitude') \
                    .dropDuplicates()
    
    artists_table.createOrReplaceTempView('artists')
    
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
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    action_filter = df.filter(df.page == 'NextSong') \
        .select('ts', 'userId', 'level', 'song', 'artist',' sessionId', 'location', 'userAgent')

    # extract columns for users table    
    user_table = df.select('user_id', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    
    user_table.createOrReplaceTempView('user')
    
    user_table.write.parquet(os.path.join(output_data, 'user/parquet.user'), 'overWrite')
    
    # write users table to parquet files
    artists_table = df.select('artist_id', 'name', 'location', 'lattitude', 'longitude').dropDuplicates()
    
    artists_table.createOrReplaceTempView('artist')
    
    artist_table.write.parquet(os.path.join(output_data, 'artist/partquet.artist'), 'overWrite')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    time_df = action_filter.withColumn('timestamp',get_timestamp(action_filter.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime))
    df = action_filter.withColumn('datetime', get_datetime(action_filter.ts))
    
    # extract columns to create time table
    # Pulling in datetime and manually converted each time range into seperated column
    time_table = action_filter.select('datetime') \
                           .withColumn('start_time', action_filter.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')) \
                           .dropDuplicates()
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month')\
        .parquet(os.path.join(output_date, 'time/parquet.time'), 'overwrite')
    
    # read in song data to use for songplays table
    song_data = 's3://udacity-dend/song_data'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songdf = df.alias('song')
    actiondf = action_filter.alias('action')
    
    songplay = action.filter.join(songdf, col('song.artist') == col('action.artist_name'), 'inner')
    songPlay_Table = songplay.select(col('song.artist_id').alias('artist_id'),
                                    col('song.song_id').alias('song_id'),
                                    col('action.level').alias('level'),
                                    col('action.user_id').alias('user_id'),
                                    col('action.datetime').alias('start_time'),
                                     col('action.userAgent').alias('user_agent'),
                                     col('action.location').alias('location'),
                                     col('action.sessionId').alias('sessionId')
                                    ).withColumn('songplay_id', monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songPlay_Table.createOrReplaceTempView('songplay')
    songplays_table.write.partitionBy('year','month')\
                                    .parquet(os.path.join(output_data, 'songPlay/parquet.songPlay'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
