#!/bin/bash


if [ ! -f shared-vol/data/meteo-data/parquet.tgz  ]; then

  wget -P shared-vol/data/meteo-data -q https://storage.googleapis.com/spark-weather-data/flag_description.csv
  wget -P shared-vol/data/meteo-data https://storage.googleapis.com/spark-weather-data/observation_type.csv
  wget -P shared-vol/data/meteo-data https://storage.googleapis.com/spark-weather-data/stations.csv
  wget -P shared-vol/data/meteo-data https://storage.googleapis.com/spark-weather-data/parquet.tgz

  tar -zxf shared-vol/data/meteo-data/parquet.tgz --directory shared-vol/data/meteo-data
#  rm -rf shared-vol/data/meteo-data/parquet.tgz

fi
