export SQLREC_VERSION="${SQLREC_VERSION:-0.1.13}"

# Base paths
# Resolve the sourced file without changing directories in zsh. An interactive
# zsh may have a chpwd hook that writes to stdout; doing this via `cd` inside a
# command substitution would capture that output and corrupt SCRIPT_DIR.
if [ -n "${ZSH_VERSION:-}" ]; then
    script_path="${(%):-%x}"
    export SCRIPT_DIR="${script_path:A:h}"
    unset script_path
elif [ -n "${BASH_VERSION:-}" ]; then
    export SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
else
    echo "ERROR: deploy/env.sh must be sourced from Bash or zsh." >&2
    return 1 2>/dev/null || exit 1
fi
export BASE_DIR="${BASE_DIR:-${SCRIPT_DIR}}"
export DATA_DIR="${BASE_DIR}/data"
export CONF_DIR="${DATA_DIR}/conf"
export LIB_DIR="${DATA_DIR}/lib"
export CLIENT_DIR="${DATA_DIR}/client"
export PV_DIR="${DATA_DIR}/pv"
export IMAGE_CACHE_DIR="${DATA_DIR}/image-cache"

source "${SCRIPT_DIR}/functions.sh"
source "${SCRIPT_DIR}/platform.sh"
detect_deploy_platform || { return 1 2>/dev/null || exit 1; }

# Kubernetes and Minikube defaults
export LIB_PV_NAME=sqlrec-lib-pv
export LIB_PVC_NAME=sqlrec-lib-pvc
export CLIENT_PV_NAME=sqlrec-client-pv
export CLIENT_PVC_NAME=sqlrec-client-pvc

export NAMESPACE="${NAMESPACE:-sqlrec}"

# unified timeout (seconds) for all deployment waits; overridable from the environment, default 1 hour
export DEPLOY_TIMEOUT="${DEPLOY_TIMEOUT:-3600}"

configure_cluster_address

export MINIKUBE_URL="https://storage.googleapis.com/minikube/releases/latest/minikube-linux-${DEPLOY_ARCH}"
export MINIKUBE_ARCH_NAME="minikube-linux-${DEPLOY_ARCH}"

configure_minikube_resources || { return 1 2>/dev/null || exit 1; }
export MINIKUBE_DISK_SIZE="${MINIKUBE_DISK_SIZE:-256gb}"
export LOCAL_PATH_PROVISIONER_DATA_DIR="${LOCAL_PATH_PROVISIONER_DATA_DIR:-/data/local-path-provisioner}"
export LOCAL_PATH_PROVISIONER_CHART="${LOCAL_PATH_PROVISIONER_CHART:-oci://ghcr.io/rancher/local-path-provisioner/charts/local-path-provisioner}"
export LOCAL_PATH_PROVISIONER_VERSION="${LOCAL_PATH_PROVISIONER_VERSION:-0.0.37}"

export DEBIAN_IMAGE_VERSION="${DEBIAN_IMAGE_VERSION:-12-slim}"

# Component versions, ports, and credentials
# all service ports are allocated sequentially from 30000; minikube maps the entire range at once
export PORT_RANGE_START=30000
export PORT_RANGE_END=30099

export HDFS_NAMENODE_PORT="${HDFS_NAMENODE_PORT:-30010}"
export HDFS_DATANODE_PORT="${HDFS_DATANODE_PORT:-30011}"
export HDFS_NAMENODE_HTTP_PORT="${HDFS_NAMENODE_HTTP_PORT:-30012}"
export HDFS_DATANODE_HTTP_PORT="${HDFS_DATANODE_HTTP_PORT:-30013}"
export HDFS_NAMENODE_DATA_DIR=${DATA_DIR}/hdfs/namenode
export HDFS_DATANODE_DATA_DIR=${DATA_DIR}/hdfs/datanode
export HDFS_NAMENODE_PV_NAME=sqlrec-hdfs-namenode-pv
export HDFS_NAMENODE_PVC_NAME=sqlrec-hdfs-namenode-pvc
export HDFS_DATANODE_PV_NAME=sqlrec-hdfs-datanode-pv
export HDFS_DATANODE_PVC_NAME=sqlrec-hdfs-datanode-pvc

export HIVE_VERSION="${HIVE_VERSION:-3.1.3}"
export HMS_POSTGRESQL_PORT="${HMS_POSTGRESQL_PORT:-30007}"
export HMS_POSTGRESQL_USER="${HMS_POSTGRESQL_USER:-metastore}"
export HMS_POSTGRESQL_PASSWORD="${HMS_POSTGRESQL_PASSWORD:-abc123456}"
export HMS_PORT="${HMS_PORT:-30008}"

export KYUUBI_VERSION="${KYUUBI_VERSION:-1.9.0}"
export KYUUBI_PORT="${KYUUBI_PORT:-30009}"

export FLINK_VERSION="${FLINK_VERSION:-1.19}"
export FLINK_API_VERSION="${FLINK_API_VERSION:-v1_19}"
export SQL_GATEWAY_PORT="${SQL_GATEWAY_PORT:-30018}"
export FLINK_JOBMANAGER_PORT="${FLINK_JOBMANAGER_PORT:-30019}"

export JUICEFS_REDIS_PORT="${JUICEFS_REDIS_PORT:-30016}"
export MINIO_PORT="${MINIO_PORT:-30014}"
export MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-30015}"
export MINIO_USER="${MINIO_USER:-rootuser}"
export MINIO_PASSWORD="${MINIO_PASSWORD:-rootpass123}"

export KAFKA_VERSION="${KAFKA_VERSION:-4.3.0}"
export KAFKA_METADATA_VERSION="${KAFKA_METADATA_VERSION:-4.1-IV1}"
export KAFKA_PORT="${KAFKA_PORT:-30021}"

export VALKEY_VERSION="${VALKEY_VERSION:-9.0.2}"
export REDIS_PORT="${REDIS_PORT:-30017}"

# Redis cluster mode deployment
# Each node binds to BASE_PORT + ordinal (e.g. 30040, 30041, ... 30055)
# Bus port is automatically PORT + 10000 (e.g. 40040, 40041, ... 40055)
export REDIS_CLUSTER_BASE_PORT="${REDIS_CLUSTER_BASE_PORT:-30040}"
export REDIS_CLUSTER_NODES="${REDIS_CLUSTER_NODES:-3}"
# bus port range for minikube --ports mapping
export REDIS_CLUSTER_BUS_PORT_RANGE_START=$((REDIS_CLUSTER_BASE_PORT + 10000))
export REDIS_CLUSTER_BUS_PORT_RANGE_END=$((REDIS_CLUSTER_BASE_PORT + REDIS_CLUSTER_NODES - 1 + 10000))

export MILVUS_VERSION="${MILVUS_VERSION:-v2.6.2}"
export MILVUS_PORT="${MILVUS_PORT:-30022}"

export TEST_POSTGRESQL_PORT="${TEST_POSTGRESQL_PORT:-30006}"
export TEST_POSTGRESQL_USER="${TEST_POSTGRESQL_USER:-test}"
export TEST_POSTGRESQL_PASSWORD="${TEST_POSTGRESQL_PASSWORD:-abc123456}"

export SQLREC_POSTGRESQL_PORT="${SQLREC_POSTGRESQL_PORT:-30005}"
export SQLREC_POSTGRESQL_USER="${SQLREC_POSTGRESQL_USER:-sqlrec}"
export SQLREC_POSTGRESQL_PASSWORD="${SQLREC_POSTGRESQL_PASSWORD:-abc123456}"
export SQLREC_THRIFT_PORT="${SQLREC_THRIFT_PORT:-30000}"
export SQLREC_REST_PORT="${SQLREC_REST_PORT:-30001}"
export SQLREC_DEBUG_PORT="${SQLREC_DEBUG_PORT:-30002}"

export JUPYTERHUB_VERSION="${JUPYTERHUB_VERSION:-4.3.1}"
export JUPYTERHUB_PORT="${JUPYTERHUB_PORT:-30028}"
export JUPYTERHUB_USER="${JUPYTERHUB_USER:-sqlrec}"
export JUPYTERHUB_PASSWORD="${JUPYTERHUB_PASSWORD:-abc123456}"

export MONGODB_VERSION="${MONGODB_VERSION:-7.0}"
export MONGODB_PORT="${MONGODB_PORT:-30029}"
export MONGODB_USER="${MONGODB_USER:-sqlrec}"
export MONGODB_PASSWORD="${MONGODB_PASSWORD:-abc123456}"

export GROWTHBOOK_VERSION="${GROWTHBOOK_VERSION:-4.4.0}"
export GROWTHBOOK_MONGODB_PORT="${GROWTHBOOK_MONGODB_PORT:-30030}"
export GROWTHBOOK_MONGODB_USER="${GROWTHBOOK_MONGODB_USER:-sqlrec}"
export GROWTHBOOK_MONGODB_PASSWORD="${GROWTHBOOK_MONGODB_PASSWORD:-abc123456}"
export GROWTHBOOK_NODE_ENV=production
export GROWTHBOOK_JWT_SECRET="${GROWTHBOOK_JWT_SECRET:-sqlrec_growthbook_jwt_secret}"
export GROWTHBOOK_ENCRYPTION_KEY="${GROWTHBOOK_ENCRYPTION_KEY:-sqlrec_growthbook_encryption_key}"
export GROWTHBOOK_WEB_PORT="${GROWTHBOOK_WEB_PORT:-30031}"
export GROWTHBOOK_API_PORT="${GROWTHBOOK_API_PORT:-30032}"

export DOLPHINSCHEDULER_VERSION="${DOLPHINSCHEDULER_VERSION:-3.4.1}"
export DOLPHINSCHEDULER_POSTGRESQL_PORT="${DOLPHINSCHEDULER_POSTGRESQL_PORT:-30033}"
export DOLPHINSCHEDULER_POSTGRESQL_USER="${DOLPHINSCHEDULER_POSTGRESQL_USER:-sqlrec}"
export DOLPHINSCHEDULER_POSTGRESQL_PASSWORD="${DOLPHINSCHEDULER_POSTGRESQL_PASSWORD:-abc123456}"
export DOLPHINSCHEDULER_PORT="${DOLPHINSCHEDULER_PORT:-30034}"

export CLICKHOUSE_VERSION="${CLICKHOUSE_VERSION:-25.3}"
export CLICKHOUSE_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-30024}"
export CLICKHOUSE_TCP_PORT="${CLICKHOUSE_TCP_PORT:-30023}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-abc123456}"

export MYSQL_VERSION="${MYSQL_VERSION:-8.4}"
export MYSQL_NAME=mysql
export MYSQL_PORT="${MYSQL_PORT:-30025}"
export MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-abc123456}"
export MYSQL_DATABASE=sqlrec
export MYSQL_USER="${MYSQL_USER:-sqlrec}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-abc123456}"

export GRAFANA_PORT="${GRAFANA_PORT:-30035}"
export PROMETHEUS_PORT="${PROMETHEUS_PORT:-30036}"

export JAEGER_VERSION="${JAEGER_VERSION:-2.19.0}"
export JAEGER_UI_PORT="${JAEGER_UI_PORT:-30037}"
export JAEGER_OTLP_GRPC_PORT="${JAEGER_OTLP_GRPC_PORT:-30038}"
export JAEGER_OTLP_HTTP_PORT="${JAEGER_OTLP_HTTP_PORT:-30039}"

export OPENSEARCH_VERSION="${OPENSEARCH_VERSION:-2.19.1}"
export OPENSEARCH_HTTP_PORT="${OPENSEARCH_HTTP_PORT:-30026}"
export OPENSEARCH_PASSWORD="${OPENSEARCH_PASSWORD:-Sqlrec_123456}"
export OPENSEARCH_DASHBOARDS_PORT="${OPENSEARCH_DASHBOARDS_PORT:-30027}"

# Downloadable clients and libraries
export JFS_LATEST_TAG="${JFS_LATEST_TAG:-1.3.1}"
export JUICEFS_PLATFORM="${DEPLOY_OS}"
export JUICEFS_URL="https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-${JFS_LATEST_TAG}-${JUICEFS_PLATFORM}-${DEPLOY_ARCH}.tar.gz"
export JUICEFS_ARCH_NAME="juicefs-${JFS_LATEST_TAG}-${JUICEFS_PLATFORM}-${DEPLOY_ARCH}.tar.gz"
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

export SQLREC_FLINK_JAR_URL="https://github.com/sqlrec/sqlrec/releases/download/v${SQLREC_VERSION}/sqlrec-flink-${SQLREC_VERSION}.jar"
export SQLREC_FLINK_JAR_NAME=sqlrec-flink-${SQLREC_VERSION}.jar

export POSTGRESQL_CONNECTOR_JAR_URL=https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar
export POSTGRESQL_CONNECTOR_JAR_NAME=postgresql-42.7.8.jar

export SPARK_CLIENT_URL=https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
export SPARK_CLIENT_ARCH_NAME=spark-3.5.1-bin-hadoop3.tgz
export SPARK_CLIENT_DIR_NAME=spark-3.5.1-bin-hadoop3
export SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.1-scala2.12-java17-python3-r-ubuntu}"

export KYUUBI_CLIENT_URL="https://archive.apache.org/dist/kyuubi/kyuubi-${KYUUBI_VERSION}/apache-kyuubi-${KYUUBI_VERSION}-bin.tgz"
export KYUUBI_CLIENT_ARCH_NAME="apache-kyuubi-${KYUUBI_VERSION}-bin.tgz"
export KYUUBI_CLIENT_DIR_NAME="apache-kyuubi-${KYUUBI_VERSION}-bin"

export JAVA_VERSION="8.472.08.1"
configure_java_distribution

# Runtime locations
export HADOOP_HOME=${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}
export HIVE_HOME=${CLIENT_DIR}/${HIVE_CLIENT_DIR_NAME}
export SPARK_HOME=${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME}
export KYUUBI_HOME=${CLIENT_DIR}/${KYUUBI_CLIENT_DIR_NAME}
export JAVA_HOME=${CLIENT_DIR}/${JAVA_CLIENT_DIR_NAME}
export CONTAINER_JAVA_HOME=${CLIENT_DIR}/${CONTAINER_JAVA_DIR_NAME}
append_path "${CLIENT_DIR}"
append_path "${HADOOP_HOME}/bin"
append_path "${SPARK_HOME}/bin"
append_path "${HIVE_HOME}/bin"
append_path "${JAVA_HOME}/bin"
configure_host_tools
