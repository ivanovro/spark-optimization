class TimeOfDayTransformer(Transformer):
    """Compute the hour of day."""

    def _transform(self, dataset):
        return (
            dataset
            .withColumn('pickup_timestamp',
                        sf.to_timestamp('pickup_datetime',
                                        format='yyyy-MM-dd HH:mm:ss Z'))
            # Note: pickup hour won't be in UTC but machine timezone
            .withColumn('pickup_hour', sf.hour('pickup_timestamp'))
            .drop('pickup_timestamp')
        )


(
    TimeOfDayTransformer()
    .transform(taxi_sdf)
    .limit(10).toPandas()
)
