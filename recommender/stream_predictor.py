import sys

import pyspark.sql.functions as func
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql import types

cluster_seeds = ['199.60.17.212']
keyspace = 'movielens_small'
kafka_server = '199.60.17.212:9092'
kafka_input_topic = 'tag_rate_small'
# kafka_input_topic = '4'
kafka_output_topic = 'predictions'
trigger_time = '5 seconds'
max_offset_per_trigger = 20

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

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", kafka_input_topic) \
    .option("maxOffsetsPerTrigger", max_offset_per_trigger) \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df = df.select(func.from_json(df['value'], schema=df_schema).alias('parsed'))

df.printSchema()

# df = df.select('parsed.user_id', 'parsed.movie_id', 'parsed.value', 'parsed.timestamp', 'parsed.event_type')
df = df.select('parsed.user_id', 'parsed.movie_id')
df = df.selectExpr("CAST(user_id AS INTEGER)", "CAST(movie_id AS INTEGER)")

keyspace = 'movielens_small'

movies = spark \
    .read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table='movies', keyspace=keyspace) \
    .load()

movies.cache()

pred_df = model.transform(df)

df_joined = pred_df.join(movies, pred_df['movie_id'] == movies['movie_id'], 'inner')

df_joined = df_joined.select(df_joined['user_id'], pred_df['movie_id'],
                             df_joined['prediction'], df_joined['title'], df_joined['genres'])

df_joined = df_joined.withColumn('key', func.concat_ws('|', df_joined['user_id'],
                                                       df_joined['movie_id'],
                                                       df_joined['prediction']))
df_joined = df_joined.withColumn('value_json',
                                 func.to_json(
                                     func.struct(df_joined['user_id'], df_joined['movie_id'],
                                                 df_joined['prediction'], df_joined['title'], df_joined['genres'])))

query = df_joined.selectExpr("CAST(key AS STRING)", "CAST(value_json AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("topic", kafka_output_topic) \
    .option("checkpointLocation", 'spark-checkpoint') \
    .trigger(processingTime=trigger_time)

query.start().awaitTermination()

# kafka-console-producer --broker-list 199.60.17.212:9092 --topic 4
# pyspark --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0
# kafka-console-consumer --bootstrap-server 199.60.17.212:9092 --from-beginning --topic predictions
#spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 recommender/stream_predict.py
