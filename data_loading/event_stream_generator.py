import sys

import pyspark.sql.functions as func
from pyspark.sql import SparkSession, functions

kafka_server = '199.60.17.212:9092'
kafka_topic = 'tag_rate_small'

cluster_seeds = ['199.60.17.212']

spark = SparkSession.builder \
    .appName('Simulate Input Data') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .getOrCreate()

assert spark.version >= '2.3'  # make sure we have Spark 2.3+
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

input_dir = 'dataset/ml-latest-small'
keyspace = 'movielens_small'



movies_file_path = input_dir + '/ratings.csv'
df_rating = spark.read.format("csv").option("header", "true").load(movies_file_path)
df_rating = df_rating.withColumnRenamed("userId", "user_id")
df_rating = df_rating.withColumnRenamed("movieId", "movie_id")
df_rating = df_rating.withColumn("timestamp", functions.from_unixtime(df_rating['timestamp']))
df_rating = df_rating.withColumnRenamed('rating', 'value')
df_rating = df_rating.withColumn('event_type', func.lit('rate'))

links_file_path = input_dir + '/tags.csv'
df_tags = spark.read.format("csv").option("header", "true").load(links_file_path)
df_tags = df_tags.withColumnRenamed("movieId", "movie_id")
df_tags = df_tags.withColumnRenamed("userId", "user_id")
df_tags = df_tags.withColumnRenamed("movieId", "movie_id")
df_tags = df_tags.withColumn("timestamp", functions.from_unixtime(df_tags['timestamp']))
df_tags = df_tags.withColumnRenamed('tag', 'value')
df_tags = df_tags.withColumn('event_type', func.lit('tag'))

all_data = df_rating.unionAll(df_tags)
all_data_sorted = all_data.sort(all_data['timestamp'].asc())
all_data_sorted = all_data_sorted.withColumn('key',
                                             func.concat_ws('|', all_data_sorted['user_id'],
                                                            all_data_sorted['movie_id'],
                                                            all_data_sorted['value'], all_data_sorted['timestamp'],
                                                            all_data_sorted['event_type']))
all_data_sorted = all_data_sorted.withColumn('value_json',
                                             func.to_json(
                                                 func.struct(all_data_sorted['user_id'], all_data_sorted['movie_id'],
                                                             all_data_sorted['value'], all_data_sorted['timestamp'],
                                                             all_data_sorted['event_type'])))

all_data_sorted.selectExpr("CAST(key AS STRING)", "CAST(value_json AS STRING) as value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("topic", kafka_topic) \
    .save()

# kafka-console-consumer --bootstrap-server 199.60.17.212:9092 --topic tag_rate_small --from-beginning
# {"user_id":"514","movie_id":"5247","value":"2.5","timestamp":"2018-09-23 19:44:00","event_type":"rate"}
#spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 data_loading/event_stream_generator.py

