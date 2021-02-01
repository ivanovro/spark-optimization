import argparse
import pyspark
import pyspark.sql.functions as sf
import pyspark.sql.types as st


def main(df):
    results = (
     input_df.select(sf.explode(sf.split(sf.col("_c4"), " ")).alias("word"))
      .withColumn('nr', sf.lit(1))
      .groupBy('word')
      .agg(sf.sum('nr').alias('sum')).orderBy(sf.col('sum').desc())
    )
    return results


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='This is a demo application.')
    parser.add_argument('-i', '--input',  help='Input file name',  required=True)
    args = parser.parse_args()

    spark = (
        pyspark.sql.SparkSession.builder
               .getOrCreate()
    )

    input_df = (
        spark.read
             .option("header", "false")
             .csv(args.input)
    )
    input_df.show()

    counts = main(input_df)
    counts.show(truncate=False)

    spark.sparkContext.stop()
