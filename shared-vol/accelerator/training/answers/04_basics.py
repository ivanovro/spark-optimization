import time

rounded_counts = (
    values
    .withColumn('rounded_x', sf.round('x'))
    .groupby('rounded_x')
    .count()
    .sort('rounded_x')
)

sleep_for = 5

query = (
    rounded_counts
    .writeStream
    .trigger(processingTime=f'{sleep_for} seconds')
    .format('memory')
    .outputMode('complete')
    .queryName('basics')
    .start()
)

time.sleep(sleep_for)
spark.table('basics').toPandas()

query.stop()
query.awaitTermination(2)
