# -- Software Stack Version

$SPARK_VERSION = "3.0.0"
$HADOOP_VERSION = "2.7"
$JUPYTERLAB_VERSION = "2.1.5"

# -- Building the Images

docker build -f Dockerfile.cluster_base -t cluster-base .

docker build --build-arg spark_version=$SPARK_VERSION --build-arg hadoop_version=$HADOOP_VERSION -f Dockerfile.spark_base -t spark-base .

docker build -f Dockerfile.spark_master -t spark-master  .

docker build -f Dockerfile.spark_history -t spark-history .

docker build -f Dockerfile.spark_worker -t spark-worker  .

docker build  --build-arg spark_version="$SPARK_VERSION"  --build-arg jupyterlab_version="$JUPYTERLAB_VERSION" -f Dockerfile.jupyter_lab -t jupyterlab  .
 