# 1
year_window = Window.partitionBy('year')

(
    airlines
    .dropna(subset='arr_delay')
    .withColumn('demean_year',
                sf.col('arr_delay') - sf.avg('arr_delay').over(year_window))
    .select('year', 'carrier', 'demean_year')
    .toPandas()
)


# 2

year_carrier_window = Window.partitionBy('year', 'carrier')

(
    airlines
    .dropna(subset='arr_delay')
    .withColumn('demean_year_carrier',
                sf.col('arr_delay') -
                sf.avg('arr_delay').over(year_carrier_window))
    .select('year', 'carrier', 'demean_year_carrier')
    .toPandas()
)


# 3

cancelled = (
    airlines
    .dropna(subset='arr_cancelled')
    .groupby('year', 'carrier')
    .agg(sf.sum('arr_cancelled').alias('n_cancelled'))
)
rank_window = Window.partitionBy('year').orderBy(sf.desc('n_cancelled'))
(
    cancelled
    .withColumn('rank', sf.rank().over(rank_window))
    .filter(sf.col('rank') <= 5)
    .sort('year', 'rank', ascending=True)
    .toPandas()
)

cancelled = (
    airlines
    .dropna(subset='arr_cancelled')
    .groupby('year', 'carrier')
    .agg(sf.sum('arr_cancelled').alias('n_cancelled'),
         sf.sum('arr_flights').alias('n_flights'))
    .withColumn('ratio', sf.col('n_cancelled') / sf.col('n_flights'))
)
rank_window = Window.partitionBy('year').orderBy(sf.desc('ratio'))
(
    cancelled
    .withColumn('rank', sf.rank().over(rank_window))
    .filter(sf.col('rank') <= 5)
    .sort('year', 'rank', ascending=True)
    .toPandas()
)


# 5

delay_window = Window.partitionBy('carrier').orderBy(sf.desc('security_delay'))
(
    airlines
    .dropna()
    .withColumn('delay_rank', sf.rank().over(delay_window))
    .filter(sf.col('delay_rank') == 1)
    .select('carrier', 'airport', 'security_delay', 'delay_rank')
).toPandas()
