#!/bin/bash

# Clean docker environment, remove all course related objects.

# remove containers
docker rm spark-worker-1 spark-worker-2 spark-master spark-history jupyterlab sparklint

# remove images and prune dangling
#docker image rm cluster-base spark-base spark-master spark-worker spark-history jupyterlab sparklint
#docker image prune -f

# remove named volumes
docker volume rm --force spark-optimisation-training_shared-workspace
docker volume rm --force docker_shared-workspace
docker volume rm docker-local_shared-workspace
