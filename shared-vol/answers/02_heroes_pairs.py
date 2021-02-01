(
    heroes.alias("h1").join(heroes.alias("h2"), sf.col("h1.role") != sf.col("h2.role"))
        .withColumn("total_hp", sf.col("h1.hp") + sf.col("h2.hp"))
        .select(sf.col("h1.name").alias("h1name"),
                sf.col("h2.name").alias("h2name"),
                sf.col("total_hp")
                )
        .where(sf.col("h1name") > sf.col("h2name"))
        .orderBy(sf.col("total_hp"), ascending=False)
        .show()
)