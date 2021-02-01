# 1.

window = Window.partitionBy('airport', 'carrier').orderBy('year', 'month')
(
    airlines
    .withColumn('arr_flights_diff',
                sf.col('arr_flights') - sf.lag('arr_flights').over(window))
    .fillna(0, subset=['arr_flights_diff'])
    .select('airport', 'carrier', 'year', 'month', 'arr_flights',
            'arr_flights_diff')
    .limit(20).toPandas()
)


# 2

window = Window.partitionBy('airport', 'carrier', 'year')
delay_threshold = 1000
f_threshold = 0.2
(
    airlines
    .withColumn('is_above_delay_threshold',
                (sf.col('arr_delay') > delay_threshold).cast('int'))
    .withColumn('n_in_window', sf.count('*').over(window))
    .withColumn('pct_above_delay_threshold',
                (sf.sum('is_above_delay_threshold').over(window)
                 / sf.col('n_in_window')))
    .filter(sf.col('pct_above_delay_threshold') < f_threshold)
    .select('airport', 'carrier', 'year', 'arr_delay',
            'pct_above_delay_threshold')
    .limit(20).toPandas()
)


# 3. Single column

historical_window = (
    Window
    .partitionBy('airport', 'carrier')
    .orderBy('year', 'month')
    .rangeBetween(-sys.maxsize, 0)
)
(
    airlines
    .withColumn('minimum', sf.min('arr_flights').over(historical_window))
    .withColumn('filled',
                sf.when(sf.isnan('arr_flights'), sf.col('minimum'))
                .otherwise(sf.col('arr_flights')))
    .filter(sf.col('carrier') == 'F9')
    .select('airport', 'carrier', 'year', 'month', 'arr_flights', 'filled')
    .limit(10)
    .toPandas()
)


# 3. Multiple columns

def fill_col(col):
    window = (
        Window
        .partitionBy('airport', 'carrier')
        .orderBy('year', 'month')
        .rangeBetween(-sys.maxsize, 0)
    )
    minimum = sf.min(col).over(window)
    filled = sf.when(sf.isnan(col), minimum).otherwise(sf.col(col))
    return filled


filled_airlines = airlines
for column in ['arr_flights', 'arr_del15', 'carrier_ct', 'weather_ct']:
    filled_airlines = (
        filled_airlines
        .withColumn('filled_{}'.format(column), fill_col(column))
    )
filled_airlines.filter(sf.col('carrier') == 'F9').limit(20).toPandas()