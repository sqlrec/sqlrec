export SQLREC_VERSION="${SQLREC_VERSION:-0.1.1}"

export SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
export BASE_DIR=${BASE_DIR:-${SCRIPT_DIR}}
export DATA_DIR=${BASE_DIR}/data
export CONF_DIR=${DATA_DIR}/conf
export LIB_DIR=${DATA_DIR}/lib
export CLIENT_DIR=${DATA_DIR}/client
export PV_DIR=${DATA_DIR}/pv
export LOCAL_REGISTRY_DIR=${DATA_DIR}/registry

export LIB_PV_NAME=sqlrec-lib-pv
export LIB_PVC_NAME=sqlrec-lib-pvc
export CLIENT_PV_NAME=sqlrec-client-pv
export CLIENT_PVC_NAME=sqlrec-client-pvc

export NAMESPACE="${NAMESPACE:-sqlrec}"

export IMAGE_REGISTRY_PORT=5000
export IMAGE_REGISTRY_URL="host.minikube.internal:${IMAGE_REGISTRY_PORT}"
if command -v kubectl &> /dev/null; then
    export NODE_IP=`kubectl get node -o wide | awk 'NR==2{print $6}'`
    export K8S_APISERVER_ADDR=k8s://https://${NODE_IP}:8443
fi

export MINIKUBE_URL=https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64

export DEPLOY_HDFS="${DEPLOY_HDFS:-false}"
export HDFS_NAMENODE_PORT=32400
export HDFS_DATANODE_PORT=32401
export HDFS_NAMENODE_HTTP_PORT=32402
export HDFS_DATANODE_HTTP_PORT=32403
export HDFS_NAMENODE_DATA_DIR=${DATA_DIR}/hdfs/namenode
export HDFS_DATANODE_DATA_DIR=${DATA_DIR}/hdfs/datanode
export HDFS_NAMENODE_PV_NAME=sqlrec-hdfs-namenode-pv
export HDFS_NAMENODE_PVC_NAME=sqlrec-hdfs-namenode-pvc
export HDFS_DATANODE_PV_NAME=sqlrec-hdfs-datanode-pv
export HDFS_DATANODE_PVC_NAME=sqlrec-hdfs-datanode-pvc

export HMS_POSTGRESQL_PORT=30307
export HMS_POSTGRESQL_USER=metastore
export HMS_POSTGRESQL_PASSWORD=abc123456
export HMS_PORT=32083
export KYUUBI_PORT=32009
export SQL_GATEWAY_PORT=30000

export JUICEFS_REDIS_PORT=30306
export MINIO_PORT=32000
export MINIO_CONSOLE_PORT=32001
export MINIO_USER=rootuser
export MINIO_PASSWORD=rootpass123

export KAFKA_PORT=32092
export REDIS_PORT=32379
export MILVUS_PORT=31530
export TEST_POSTGRESQL_PORT=30309
export TEST_POSTGRESQL_USER=test
export TEST_POSTGRESQL_PASSWORD=abc123456

export SQLREC_POSTGRESQL_PORT=30308
export SQLREC_POSTGRESQL_USER=sqlrec
export SQLREC_POSTGRESQL_PASSWORD=abc123456
export SQLREC_THRIFT_PORT=30300
export SQLREC_REST_PORT=30301
export SQLREC_DEBUG_PORT=30302

export JUPYTERHUB_PORT=30280
export JUPYTERHUB_USER=sqlrec
export JUPYTERHUB_PASSWORD=abc123456

export MONGODB_PORT=30281
export MONGODB_USER=sqlrec
export MONGODB_PASSWORD=abc123456
export GROWTHBOOK_MONGODB_PORT=30282
export GROWTHBOOK_MONGODB_USER=sqlrec
export GROWTHBOOK_MONGODB_PASSWORD=abc123456
export GROWTHBOOK_MONGODB_USER=sqlrec

export GROWTHBOOK_WEB_PORT=30283
export GROWTHBOOK_API_PORT=30284

export DOLPHINSCHEDULER_VERSION="3.4.1"
export DOLPHINSCHEDULER_POSTGRESQL_PORT=30305
export DOLPHINSCHEDULER_POSTGRESQL_USER=sqlrec
export DOLPHINSCHEDULER_POSTGRESQL_PASSWORD=abc123456
export DOLPHINSCHEDULER_PORT=30287

export GRAFANA_PORT=30285
export PROMETHEUS_PORT=30286

export JFS_LATEST_TAG=1.3.1
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

export POSTGRESQL_CONNECTOR_JAR_URL=https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar
export POSTGRESQL_CONNECTOR_JAR_NAME=postgresql-42.7.8.jar

export SPARK_CLIENT_URL=https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
export SPARK_CLIENT_ARCH_NAME=spark-3.5.1-bin-hadoop3.tgz
export SPARK_CLIENT_DIR_NAME=spark-3.5.1-bin-hadoop3

export JAVA_CLIENT_URL=https://corretto.aws/downloads/resources/8.472.08.1/amazon-corretto-8.472.08.1-linux-x64.tar.gz
export JAVA_CLIENT_ARCH_NAME=amazon-corretto-8.472.08.1-linux-x64.tar.gz
export JAVA_CLIENT_DIR_NAME=amazon-corretto-8.472.08.1-linux-x64

export HADOOP_HOME=${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}
export HIVE_HOME=${CLIENT_DIR}/${HIVE_CLIENT_DIR_NAME}
export SPARK_HOME=${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME}
export JAVA_HOME=${CLIENT_DIR}/${JAVA_CLIENT_DIR_NAME}
export PATH=${PATH}:${HADOOP_HOME}/bin:${SPARK_HOME}/bin:${HIVE_HOME}/bin:${JAVA_HOME}/bin
