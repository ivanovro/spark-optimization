#!/bin/sh

# Files used:
#  - airlines.parquet
#  - chicagoCensus.csv
#  - heroes.csv

set -e

if [ ! -f ds-spark-data.zip ]; then
  curl -L -O https://storage.googleapis.com/academy-data/ds-with-spark.zip
fi

unzip ds-with-spark.zip
