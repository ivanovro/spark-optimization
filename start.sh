#!/bin/bash

# Creates folders structure and launches local Docker Compose application

instruqt=0

for arg in "$@"
do
  case $arg in
     --instruqt)
     instruqt=1
     shift
     ;;
  esac
done

# make sure required volumes exist
mkdir -p shared-vol/history
mkdir -p shared-vol/logs

# initialize env vars for Docker Compose
export SPARK_WORKER_CORES=2
export SHARED_DIR=`pwd`/shared-vol

# start application

if [ $instruqt == 1 ]
then
  echo "Running instruqt app"
  docker-compose -f docker/docker-compose.yml up
else
  echo "Running local app"
  docker-compose -f docker-local/docker-compose.yml up -d
fi
