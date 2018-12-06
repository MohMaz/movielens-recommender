from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import sys

cluster_seeds = ['199.60.17.212']

spark = SparkSession.builder \
    .appName('Simulate Input Data') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .getOrCreate()

assert spark.version >= '2.3'  # make sure we have Spark 2.3+
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def main(input_dir, keyspace):
    # main logic starts here

    movies_file_path = input_dir + '/movies.csv'
    df_movies = spark.read.format("csv").option("header", "true").load(movies_file_path)
    df_movies = df_movies.withColumn('genres', func.split(df_movies['genres'], "\|"))
    df_movies = df_movies.withColumnRenamed("movieId", "movie_id")

    df_movies.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table='movies', keyspace=keyspace) \
        .save()

    links_file_path = input_dir + '/links.csv'
    df_links = spark.read.format("csv").option("header", "true").load(links_file_path)
    df_links = df_links.withColumnRenamed("movieId", "movie_id")
    df_links = df_links.withColumnRenamed("imdbId", "imdb_id")
    df_links = df_links.withColumnRenamed("tmdbId", "tmdb_id")

    df_links.show()

    df_links.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table='links', keyspace=keyspace) \
        .save()


if __name__ == '__main__':
    input_dir = 'dataset/ml-latest-small'
    keyspace = 'movielens_small'
    main(input_dir, keyspace)
