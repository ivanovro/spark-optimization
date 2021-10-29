#!/usr/bin/env bash
#
# Push recently built images to docker registry.
#

registry="europe-west1-docker.pkg.dev/spark-optimization/training"
images=("cluster-base", "spark-master", "spark-history spark-worker jupyterlab")

for image in ${images[@]}; do
  echo "--- pushing $image ---"
	docker tag $image:latest $registry/$image:latest
  docker push $registry/$image:latest
done
