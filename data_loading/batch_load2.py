from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import pyspark.sql.functions as func
from datetime import datetime
import math
import sys
import re

from uuid import uuid4

cluster_seeds = ['199.60.17.212']

spark = SparkSession.builder \
    .appName('Simulate Input Data') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .getOrCreate()

assert spark.version >= '2.3'  # make sure we have Spark 2.3+
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def main(input_dir, keyspace):
    # main logic starts here

    movies_file_path = input_dir + '/ratings.csv'
    df_rating = spark.read.format("csv").option("header", "true").load(movies_file_path)
    df_rating = df_rating.withColumnRenamed("userId", "user_id")
    df_rating = df_rating.withColumnRenamed("movieId", "movie_id")
    df_rating = df_rating.withColumn("timestamp", functions.from_unixtime(df_rating['timestamp']))

    df_rating.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table='ratings', keyspace=keyspace) \
        .save()

    links_file_path = input_dir + '/tags.csv'
    df_tags = spark.read.format("csv").option("header", "true").load(links_file_path)
    df_tags = df_tags.withColumnRenamed("movieId", "movie_id")
    df_tags = df_tags.withColumnRenamed("userId", "user_id")
    df_tags = df_tags.withColumnRenamed("movieId", "movie_id")
    df_tags = df_tags.withColumn("timestamp", functions.from_unixtime(df_tags['timestamp']))

    df_tags.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table='tags', keyspace=keyspace) \
        .save()


if __name__ == '__main__':
    input_dir = 'dataset/ml-latest-small'
    keyspace = 'movielens_small'
    main(input_dir, keyspace)
