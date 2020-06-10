1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As the data engineer, we are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


2. State and justify your database schema design and ETL pipeline.

- Song_data and log_data resides in S3 with Json formal
- Spark processing:
	+ Schema on read: read data from S3 into data frame
	+ Extract columns from data frames to create songs_table, artists_table, users_table, time_table and songplays_table
	+ write back dimentions and fact table into S3


3. [Optional] Provide example queries and results for song play analysis.



