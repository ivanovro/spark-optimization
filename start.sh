#!/bin/bash

# Launch Docker Compose application

# make sure required volumes exist
mkdir -p shared-vol/history
mkdir -p shared-vol/logs

# initialize env vars for Docker Compose
export SPARK_WORKER_CORES=2
export SHARED_DIR=`pwd`/shared-vol

# start application
docker-compose -f docker-local/docker-compose.yml up
