# Doesn't infer types correctly.
heroes = spark.read.csv(heroes_path, header=True)
heroes.dtypes

# Infers types correctly, misses NA.
heroes = spark.read.csv(heroes_path, header=True, inferSchema=True)
heroes.toPandas()

# This is what we want.
heroes = spark.read.csv(heroes_path, header=True, inferSchema=True,
                        nanValue='NA')
heroes.toPandas()


# Without Gail.
heroes = (
    spark.read.csv(heroes_path, header=True, inferSchema=True, nanValue='NA')
    .filter(~sf.isnan('attack'))
)

# Find largest HP.
heroes.sort('hp', ascending=False).toPandas()

# Add attack momentum column.
heroes.withColumn('attack_momentum', sf.col('attack') * sf.col('attack_spd'))

# Average attack.
(
    heroes
    .groupBy('role')
    .agg(sf.mean('attack').alias('avg_attack'))
    .toPandas()
)

# Co-occurence.
(
    heroes
    .groupBy('role', 'attack_type')
    .count()
    .sort('count', ascending=False)
)


# Highest attack in role.
(
    heroes
    .sort('attack')
    .groupBy('role')
    .agg(sf.first('name').alias('first_name'))
)
