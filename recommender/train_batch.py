import sys

from pyspark.ml import Pipeline

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
import datetime

from pyspark.sql import SparkSession, functions, types

cluster_seeds = ['199.60.17.212']
keyspace = 'movielens_small'

spark = SparkSession.builder \
    .appName('Train Recommender') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

assert spark.version >= '2.3'  # make sure we have Spark 2.3+
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

ratings = spark \
    .read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table='ratings', keyspace=keyspace) \
    .load()

ratings.show()

(training, test) = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=2, regParam=0.01, userCol="user_id", itemCol="movie_id", ratingCol="rating",
          coldStartStrategy="drop")


model = als.fit(training)

model_name = 'models/ALS'

model.save(model_name)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
# Generate top 10 user recommendations for each movie
movieRecs = model.recommendForAllItems(10)

# Generate top 10 movie recommendations for a specified set of users
users = ratings.select(als.getUserCol()).distinct().limit(3)
userSubsetRecs = model.recommendForUserSubset(users, 10)
# Generate top 10 user recommendations for a specified set of movies
movies = ratings.select(als.getItemCol()).distinct().limit(3)
movieSubSetRecs = model.recommendForItemSubset(movies, 10)




########3

pipeline = Pipeline(stages=[als])
model = pipeline.fit(training)

test = spark.createDataFrame([
    (449, 318),
], ["user_id", "movie_id"])

model.transform(test)