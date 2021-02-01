# 1
month_window = Window.partitionBy('month')
with_average = (
    temperatures
        .withColumn('mean_temp_month', sf.mean('temperature').over(month_window))
)


# 2
mid_window = Window.orderBy('mid')
with_previous = (
    with_average
        .withColumn('temp_delta', sf.col('temperature') -
                    sf.lag('temperature').over(mid_window))
)


# 3
with_previous.filter(sf.col('mean_temp_month') > 5).toPandas()
