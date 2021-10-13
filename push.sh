#!/usr/bin/env bash
#
# Push recently built images to docker registry.
#

for image in cluster-base spark-master spark-history spark-worker jupyterlab; do
  echo "--- pushing $image ---"
	docker tag $image:latest europe-west1-docker.pkg.dev/spark-optimization/training/$image:latest
  docker push europe-west1-docker.pkg.dev/spark-optimization/training/$image:latest
done
