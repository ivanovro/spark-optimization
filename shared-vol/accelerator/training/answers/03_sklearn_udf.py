from pyspark.sql.types import DoubleType


@sf.pandas_udf(returnType=DoubleType())
def predict_pandas_udf(*cols):
    # Cols will be a tuple of pandas.Series here.
    X = pd.concat(cols, axis=1)
    p_benign = sklearn_model.predict_proba(X)[:, 1]
    return pd.Series(p_benign)


(
    test_sdf
    .withColumn('p_begin', predict_pandas_udf(*X.columns.tolist()))
    .limit(5)
    .toPandas()
    .T
)
