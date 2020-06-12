import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import  pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    """
    This function is used to process Song Dataset from S3.
    Data is extracted from Song Dataset to create songs_table and artists_table.
    These dimension tables will be written on S3 again.
    """


    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*")
    
    # read song data file
    df = spark.read.json(song_data)

    # add new
    df.createOrReplaceTempView("song_df")
    
    ### SONGS TABLE ###
    # extract columns to create songs table
    songs_table = spark.sql(""" 
                    SELECT DISTINCT song_id, title, artist_id, year, duration
                    FROM song_df
                    WHERE song_id IS NOT NULL
                    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs_table/")
    
    
    ### ARTISTS TABLE ###
    # extract columns to create artists table
    artists_table = spark.sql("""
                    SELECT DISTINCT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
                    FROM song_df
                    WHERE artist_id IS NOT NULL
                    """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists_table/")
    
    
    
def process_log_data(spark, input_data, output_data):
    """
    This function is used to process Log Dataset from S3.
    Data is extracted from Log Dataset to create users_table and time_table.
    From both Log Dataset and Song Dataset, data will be extracted to create fact table - songplays_table.
    These dimension and fact tables will be written on S3 again.
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*")
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # add new
    df.createOrReplaceTempView("log_df")

    ### USERS TABLE ###
    # extract columns for users table    
    users_table = spark.sql("""
                    SELECT DISTINCT userId as user_id, firstName as first_name, lastName as last_name, gender, level
                    FROM log_df
                    WHERE userId IS NOT NULL
                    """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users_table/")
    
      
    ### TIME TABLE ###
    # create timestamp column from original timestamp column
    def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)
    
    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    df.createOrReplaceTempView("log_df")  
    
     # extract columns for time table    
    time_table = spark.sql("""
                    SELECT DISTINCT timestamp as start_time,
                        hour(timestamp) as hour,
                        dayofmonth(timestamp) as day,
                        weekofyear(timestamp) as week,
                        month(timestamp) as month,
                        year(timestamp) as year, 
                        date_format(timestamp, 'W') as weekday
                    FROM log_df
                    """)

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data + 'time_table/')

    # add new
    time_table.createOrReplaceTempView("time_table")
 
    
    ### SONGPLAYS TABLE ###
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                    SELECT DISTINCT l.timestamp as start_time, l.userId as user_id, l.level, s.song_id, s.artist_id, 
                            l.sessionId as session_id, l.location, l.userAgent as user_agent, t.year, t.month
                    FROM log_df as l
                    JOIN song_df as s ON (l.artist = s.artist_name)
					AND (l.length = s.duration)
                                	AND (l.song = s.title) 
		    JOIN time_table as t ON (l.timestamp = t.start_time)                    
                    """)

    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays_table/')
  

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dungthan/"
    
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
