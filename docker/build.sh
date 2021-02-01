# -- Software Stack Version

SPARK_VERSION="3.0.0"
HADOOP_VERSION="2.7"
JUPYTERLAB_VERSION="2.1.5"
PROJECT=instruqt-godatadriven

# -- Building the Images

for image in cluster-base spark-master spark-history spark-worker; do
	docker build \
		-f Dockerfile.${image//-/_} \
		-t $image \
		.
	docker tag $image:latest gcr.io/$PROJECT/$image:instruqt
	docker push gcr.io/$PROJECT/$image:instruqt
done
	
docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -f Dockerfile.spark_base \
  -t spark-base \
  .
docker tag spark-base:latest gcr.io/$PROJECT/spark-base:instruqt
docker push gcr.io/$PROJECT/spark-base:instruqt

cp -R ../shared-vol .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f Dockerfile.jupyter_lab \
  -t jupyterlab \
  .
docker tag jupyterlab:latest gcr.io/$PROJECT/jupyterlab:instruqt
docker push gcr.io/$PROJECT/jupyterlab:instruqt
rm -rf shared-vol
