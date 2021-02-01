import argparse
import pyspark
import pyspark.sql.functions as sf
import pyspark.sql.types as st


def main(df):
    results = (
        df.select(sf.explode(sf.split(sf.col("word"), r"\s+")).alias("word"))
          .withColumn('nr', sf.lit(1))
          .groupBy('word')
          .agg(sf.sum('nr').alias('sum'))
    )
    return results


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='This is a demo application.')
    parser.add_argument('-i', '--input',  help='Input file name',  required=True)
    parser.add_argument('-o', '--output', help='Output file name', required=True)
    args = parser.parse_args()

    spark = (
        pyspark.sql.SparkSession.builder
               .getOrCreate()
    )

    schema = st.StructType([st.StructField("word", st.StringType())])
    input_df = (
        spark.read
             .option("header", "false")
             .csv(args.input, schema=schema)
    )

    counts = main(input_df)
    counts.write.parquet(args.output)

    spark.sparkContext.stop()
