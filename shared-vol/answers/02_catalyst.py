data_dir = '../data'
master = 'local[2]'

import os 
import pyspark
import pyspark.sql.functions as sf

spark = (
    pyspark.sql.SparkSession.builder
    .master(master) 
    .getOrCreate()
)

heroes_path = os.path.join(data_dir, 'heroes.csv')
heroes = spark.read.csv(heroes_path, header=True)

avg_attack = (
    heroes
    .groupBy('role')
    .agg(sf.mean(sf.col('attack')).alias('avg_attack'))
    .filter(sf.col('role') == 'Warrior')
)

avg_attack.show(10)
avg_attack.explain(True)
 