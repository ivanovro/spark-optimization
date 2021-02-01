#!/bin/bash

docker rm spark-worker-1 spark-worker-2 spark-master spark-history jupyterlab sparklint
docker image rm cluster-base spark-base spark-master spark-worker spark-history jupyterlab sparklint
docker image prune -f
docker volume rm --force spark-optimisation-training_shared-workspace
docker volume rm docker_shared-workspace