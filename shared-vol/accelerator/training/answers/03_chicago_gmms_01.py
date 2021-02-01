from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=['X Coordinate', 'Y Coordinate'], outputCol='features')
crime_with_features_sdf = assembler.transform(crime_sdf)
