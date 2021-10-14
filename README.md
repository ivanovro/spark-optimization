# Spark Optimisation Training
Spark optimisation training and workshop

## Docker-based local environment

### Requirements
We need up to 8-12G memory to run all required Docker containers. Don't forget to change this setting on Docker Desktop.

### Build Docker images
This builds all images needed for the setup. You can avoid building images and use the ones set by default.
```
make build
```

### Start application
```
# download data
make get-data
# this will start Docker compose application
make up
```

## Application URLs

- [JupyterLab](http://localhost:8888)
- [Spark master](http://localhost:8080/home)
- [Spark worker I](http://localhost:8081)
- [Spark worker II](http://localhost:8082)
- [Spark Application UI](http://localhost:4040)
- [Spark history](http://localhost:18080)
- [SparkLint](http://localhost:23763)

### Restart SparkLint to get new logs
Sparklint doesn't fetch new logs automatically. To process new logs you can either add them manually through UI or restart Sparklint docker component
```
docker-compose -f docker-local/docker-compose.yml restart sparklint
```

### Restart with different number of spark.executor.cores (for the excersise)
```
SPARK_WORKER_CORES=<number of executors> docker-compose -f docker-local/docker-compose.yml up -d
```

### Cleanup Docker env
Removes all stopped containers, deletes images with intermediate layers, named volumes and downloaded data.
```
make clean
```

# Getting the data in the images when using Instruqt

To get the data in the images, you need to perform the following step in the Instruqt Terminal tab (not in the Jupyter terminal)

```
mkdir -p /tmp/data/meteo-data
wget -P /tmp/data/meteo-data https://meteo-data.s3-eu-west-1.amazonaws.com/meteo-data/flag_description.csv
wget -P /tmp/data/meteo-data https://meteo-data.s3-eu-west-1.amazonaws.com/meteo-data/observation_type.csv
wget -P /tmp/data/meteo-data https://meteo-data.s3-eu-west-1.amazonaws.com/meteo-data/stations.csv
wget -P /tmp/data/meteo-data https://meteo-data.s3-eu-west-1.amazonaws.com/meteo-data/parquet-small.zip
unzip /tmp/data/meteo-data/parquet-small.zip -d /tmp/data/meteo-data
mv /tmp/data/meteo-data/parquet-small /tmp/data/meteo-data/parquet
rm -rf /tmp/data/meteo-data/parquet-small.zip
```
