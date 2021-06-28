#!/bin/bash

WITHCSV=0

for arg in "$@"
do 
  case $arg in 
     --with-csv)
     WITHCSV=1
     shift
     ;;
  esac
done

if [ ! -f data/meteo-data/parquet.zip  ]; then

  wget -P data/meteo-data -q https://storage.googleapis.com/spark-weather-data/flag_description.csv
  wget -P data/meteo-data https://storage.googleapis.com/spark-weather-data/observation_type.csv
  wget -P data/meteo-data https://storage.googleapis.com/spark-weather-data/stations.csv
#  wget -P data/meteo-data https://storage.googleapis.com/meteo-data/parquet.zip

  unzip data/meteo-data/parquet.zip -d data/meteo-data
  rm -rf data/meteo-data/parquet.zip

  if [ $WITHCSV == 1 ]; then
    wget -P data/meteo-data https://meteo-data.s3-eu-west-1.amazonaws.com/meteo-data/csv.zip
    unzip data/meteo-data/csv.zip -d data/meteo-data
    rm -rf data/meteo-data/csv.zip    
  fi

fi
