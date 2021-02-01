# 1.
(
    airlines
    .withColumn('is_nan', sf.isnan('security_delay'))
    .filter(sf.col('is_nan'))
    .count()
)


# 2.
filled_airlines = airlines.fillna(0)


# 3.
(
    filled_airlines
    .withColumn('split_first', sf.split('airport_name', ':'))
    .withColumn('get_first', sf.col('split_first').getItem(0))
    .withColumn('split_second', sf.split('get_first', ','))
    .withColumn('state', sf.col('split_second').getItem(1))
    .select('state').distinct()
    .toPandas()
)


# 4, 5

# Let's try a UDF
state_udf = sf.udf(lambda s: s.split(':')[0][-2:], st.StringType())

airport_states = (
    airlines
    .withColumn('state', state_udf('airport_name'))
    .select('airport', 'state')
    .drop_duplicates()
)


# 6
combined = airlines.join(airport_states, on='airport')


# 7
with_weather = (
    combined
    .withColumn('weather_condition',
                sf.when(sf.col('weather_delay') > 1200, 'rainy')
                .when((sf.col('weather_delay') > 1200) &
                      (sf.col('arr_diverted') > 15), 'stormy')
                .otherwise('bright'))
)

# 8
train = (
    with_weather
    .sort('year', 'month')
    .limit(20000)
)

test = (
    with_weather
    .sort('year', 'month', ascending=False)  # Other way around.
    .limit(8515)
)


# Bonus 1.
aggregations = [sf.sum(sf.isnan(x).cast('integer')).alias(x)
                for x in airlines.columns]
airlines.agg(*aggregations).toPandas().T


# Bonus 2.
(
    airlines
    .withColumn('match', sf.regexp_extract('airport_name', ',(.*):', 1))
    .select('match')
    .toPandas()
)
(
    airlines
    .withColumn('split', sf.split('airport_name', ':|,').getItem(1))
)
