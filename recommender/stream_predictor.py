import sys

import pyspark.sql.functions as func
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql import types

cluster_seeds = ['199.60.17.212']
keyspace = 'movielens_small'
kafka_server = '199.60.17.212:9092'
kafka_topic = 'tag_rate_small'

spark = SparkSession.builder \
    .appName('On Stream Predictor') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

assert spark.version >= '2.4'  # make sure we have Spark 2.4+
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

model_name = 'models/ALS'
model = ALSModel.load(model_name)

df_schema = types.StructType([
    types.StructField('user_id', types.StringType(), False),
    types.StructField('movie_id', types.StringType(), False),
    types.StructField('value', types.StringType(), False),
    types.StructField('timestamp', types.StringType(), False),
    types.StructField('event_type', types.StringType(), False)
])

# ratings = spark \
#     .read \
#     .format("org.apache.spark.sql.cassandra") \
#     .options(table='ratings', keyspace=keyspace) \
#     .load()
#
# ratings.cache()

kafka_topic = '4'

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", kafka_topic) \
    .option("maxOffsetsPerTrigger", 10) \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# df = df.withColumn('parsed', func.from_json(df['value'], schema=df_schema))
df = df.select(func.from_json(df['value'], schema=df_schema).alias('parsed'))

df.printSchema()

# df = df.select('parsed.user_id', 'parsed.movie_id', 'parsed.value', 'parsed.timestamp', 'parsed.event_type')
df = df.select('parsed.user_id', 'parsed.movie_id')
df = df.selectExpr("CAST(user_id AS INTEGER)", "CAST(movie_id AS INTEGER)")

userSubsetRecs = model.transform(df)

query = userSubsetRecs \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .trigger(processingTime='5 seconds')

query.start().awaitTermination(10)

# kafka-console-producer --broker-list 199.60.17.212:9092 --topic 4
# pyspark --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0
