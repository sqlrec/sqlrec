export SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
export BASE_DIR=${SCRIPT_DIR}
export DATA_DIR=${BASE_DIR}/data
export CONF_DIR=${DATA_DIR}/conf
export LIB_DIR=${DATA_DIR}/lib
export CLIENT_DIR=${DATA_DIR}/client
export PV_DIR=${DATA_DIR}/pv
export LOCAL_REGISTRY_DIR=${DATA_DIR}/registry

mkdir -p ${CONF_DIR}
mkdir -p ${LIB_DIR}
mkdir -p ${CLIENT_DIR}
mkdir -p ${PV_DIR}
mkdir -p ${LOCAL_REGISTRY_DIR}

export NAMESPACE=sqlrec
export RESOURCE_PV_NAME=sqlrec-resource-pv
export RESOURCE_PVC_NAME=sqlrec-resource-pvc
export IMAGE_REGISTRY_PORT=5000
export IMAGE_REGISTRY_URL="host.minikube.internal:${IMAGE_REGISTRY_PORT}"
if command -v kubectl &> /dev/null; then
    export NODE_IP=`kubectl get node -o wide | awk 'NR==2{print $6}'`
    export K8S_APISERVER_ADDR=k8s://https://${NODE_IP}:8443
fi

export SQL_GATEWAY_PORT=30000
export HMS_MYSQL_PORT=30307
export HMS_MYSQL_USER=metastore
export HMS_MYSQL_PASSWORD=abc123456
export HMS_PORT=32083
export KYUUBI_PORT=32009
export JUICEFS_MYSQL_PORT=30306
export JUICEFS_MYSQL_USER=juicefs
export JUICEFS_MYSQL_PASSWORD=abc123456
export MINIO_PORT=32000
export MINIO_CONSOLE_PORT=32001
export MINIO_USER=rootuser
export MINIO_PASSWORD=rootpass123
export KAFKA_PORT1=32092
export REDIS_PORT=32379
export SQLREC_MYSQL_PORT=30308
export SQLREC_MYSQL_USER=sqlrec
export SQLREC_MYSQL_PASSWORD=abc123456
export MILVUS_PORT=31530

export MINIKUBE_URL=https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64

export JFS_LATEST_TAG=1.3.0
export JUICEFS_URL="https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz"
export JUICEFS_ARCH_NAME=juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz
export JUICEFS_HADOOP_JAR_URL="https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-hadoop-${JFS_LATEST_TAG}.jar"
export JUICEFS_HADOOP_JAR_NAME=juicefs-hadoop-${JFS_LATEST_TAG}.jar

export HADOOP_CLIENT_URL=https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
export HADOOP_CLIENT_ARCH_NAME=hadoop-3.4.0.tar.gz
export HADOOP_CLIENT_DIR_NAME=hadoop-3.4.0

export HIVE_CLIENT_URL=https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
export HIVE_CLIENT_ARCH_NAME=apache-hive-3.1.3-bin.tar.gz
export HIVE_CLIENT_DIR_NAME=apache-hive-3.1.3-bin

export FLINK_HADOOP_JAR_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
export FLINK_HADOOP_JAR_NAME=flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
export FLINK_SQL_CONNECTOR_HIVE_JAR_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.9_2.12/1.19.0/flink-sql-connector-hive-2.3.9_2.12-1.19.0.jar
export FLINK_SQL_CONNECTOR_HIVE_JAR_NAME=flink-sql-connector-hive-2.3.9_2.12-1.19.0.jar

export MYSQL_CONNECTOR_JAR_URL=https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar
export MYSQL_CONNECTOR_JAR_NAME=mysql-connector-java-8.0.27.jar

export SPARK_CLIENT_URL=https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
export SPARK_CLIENT_ARCH_NAME=spark-3.5.1-bin-hadoop3.tgz
export SPARK_CLIENT_DIR_NAME=spark-3.5.1-bin-hadoop3








