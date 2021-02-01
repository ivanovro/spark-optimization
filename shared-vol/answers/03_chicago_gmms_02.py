from pyspark.sql import functions as sf
from pyspark.sql.types import FloatType

# Answer udf
def get_probability(predicted_probabilities):
    """Given a list of probabilities, selects the maximum."""
    max_probability = max([float(p) for p in predicted_probabilities])
    return max_probability

get_probability_udf = sf.udf(get_probability, FloatType())

(
    model.transform(crime_with_features_sdf)
    .withColumn('probability', get_probability_udf('probability'))
    .show(10)
)
