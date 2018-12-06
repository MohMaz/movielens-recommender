import sys

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession

cluster_seeds = ['199.60.17.212']

spark = SparkSession.builder \
    .appName('Train Recommender') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

assert spark.version >= '2.3'  # make sure we have Spark 2.3+
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

keyspace = 'movielens_small'

ratings = spark \
    .read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table='ratings', keyspace=keyspace) \
    .load()

model_name = 'models/ALS'

print("=" * 50)
model = ALSModel.load(model_name)
the_user = ratings.where(ratings['user_id'] == 3).limit(1)
userSubsetRecs = model.recommendForUserSubset(the_user, 10)

print(userSubsetRecs)

userSubsetRecs.show()
