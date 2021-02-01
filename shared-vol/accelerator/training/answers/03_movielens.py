import os

import pandas as pd
from pylab import rcParams
import seaborn as sns
import matplotlib.pyplot as plt

import pyspark
import pyspark.sql.functions as sf
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS


plt.style.use('ggplot')
rcParams['figure.figsize'] = (10, 6)

DATA_DIR = '../data'
MASTER = 'local[2]'

spark = (
    pyspark.sql.SparkSession.builder
    .master(MASTER)
    .getOrCreate()
)

# Load and pre-process the ratings dataset.
ratings = spark.read.csv(os.path.join(DATA_DIR, "sample_movielens_ratings.txt"))

ratings = (
    ratings
    .withColumn("_tmp", sf.split(sf.col("_c0"), "::"))
    .withColumn("userId", sf.col("_tmp")[0].cast("int"))
    .withColumn("movieId", sf.col("_tmp")[1].cast("int"))
    .withColumn("rating", sf.col("_tmp")[2].cast("float"))
    .withColumn("timestamp", sf.col("_tmp")[3].cast("int").cast("timestamp"))
    .drop("_c0", "_tmp")
)

# Split into train/test set.
(training, test) = ratings.randomSplit([0.8, 0.2])

# Fit model.
als = ALS(
    rank=10,
    maxIter=5,
    regParam=0.01,
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    coldStartStrategy="drop")

model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction")

rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Show user recommendations.
user_recs = model.recommendForAllUsers(10)
user_recs.show()

# Show movie recommendations.
movie_recs = model.recommendForAllItems(10)
movie_recs.show()

# Recommend for specific users.
users = ratings.select(als.getUserCol()).distinct().limit(3)
user_subset_recs = model.recommendForUserSubset(users, 10)

user_subset_recs.show()

# Visualize item factors.
item_factors_df = (
    model.itemFactors
    .select("features")
    .toPandas()["features"]
    .apply(pd.Series).T
)

sns.clustermap(item_factors_df, figsize=(12, 6))

# Visualize user factors.
user_factors_df = (
    model.userFactors
    .select("features")
    .toPandas()["features"]
    .apply(pd.Series).T
)

sns.clustermap(user_factors_df, figsize=(12, 6))
