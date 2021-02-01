# Answer: train without scaling and get performance.

from pyspark.ml.evaluation import RegressionEvaluator

input_cols = ['age', 'sex', 'bmi', 'bp', 's1', 's2', 's3', 's4', 's5', 's6']
assembler = VectorAssembler(inputCols=input_cols, outputCol='features')

converted_train = (
    assembler.transform(train_sdf)
    .select(['features', 'label'])
)

# Default is standardization=True. Standardization scales columns to have zero
# mean and unit standard deviation.
model = LinearRegression(standardization=True)
model = model.fit(converted_train)
print('coefficients', model.coefficients)

converted_test = (
    assembler.transform(test_sdf)
    .select(['features', 'label'])
)
diabetes_predictions = model.transform(converted_test)

evaluator = RegressionEvaluator()
print('MSE:', evaluator.evaluate(diabetes_predictions,
                                 {evaluator.metricName: 'mse'}))
print('R2:', evaluator.evaluate(diabetes_predictions, 
                                {evaluator.metricName: 'r2'}))

# Bonus
from pyspark.ml.regression import RandomForestRegressor

forest_model = (
    RandomForestRegressor()
    .fit(converted_train)
)
diabetes_forest_predictions = forest_model.transform(converted_test)
linear_df = diabetes_predictions.select('label', 'prediction').toPandas()
forest_df = diabetes_forest_predictions.select('label', 'prediction').toPandas()

fig, ax = plt.subplots(figsize=(10, 6))
sns.scatterplot('label', 'prediction', data=linear_df, ax=ax)
sns.scatterplot('label', 'prediction', data=forest_df, ax=ax)
ax.axis('equal'); ax.legend(['linear', 'random forest'])