# Answer: Manual lag.

from pyspark.sql import Window

window = Window.orderBy(sf.col('date').asc())

weather_lag5_sdf = (
    weather_sdf
    .withColumn('lag1', sf.lag('temperature').over(window))
    .withColumn('lag2', sf.lag('temperature', 2).over(window))
    .withColumn('lag3', sf.lag('temperature', 3).over(window))
    .withColumn('lag4', sf.lag('temperature', 4).over(window))
    .withColumn('lag5', sf.lag('temperature', 5).over(window))
    .filter(sf.col('lag5').isNotNull())
)
weather_lag5_sdf.show(10)

# Answer: Loop lag.

window = Window.orderBy(sf.col('date').asc())

max_lag = 10

weather_lag10_sdf = weather_sdf
for lag in range(1, max_lag+1):
    col_name = 'lag{}'.format(lag)
    weather_lag10_sdf = (
        weather_lag10_sdf
        .withColumn(col_name, sf.lag('temperature', lag).over(window))
    )

last_col_name = 'lag{}'.format(lag)
weather_lag10_sdf = weather_lag10_sdf.dropna()

weather_lag10_sdf.show(10)