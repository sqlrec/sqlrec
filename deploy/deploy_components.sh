#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/env.sh"

if [ -z "${NODE_IP}" ]; then
  echo "ERROR: NODE_IP is not set; start Minikube first or provide NODE_IP." >&2
  exit 1
fi

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace "${NAMESPACE}-milvus" --dry-run=client -o yaml | kubectl apply -f -

render_config "${dir}/pv.yaml"
kubectl apply -f "${dir}/pv.yaml.tmp" -n "${NAMESPACE}"

# juicefs-hadoop jar must be on the hadoop/spark client classpath before hadoop/deploy.sh runs `hadoop fs` against jfs://
cp "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}" "${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/share/hadoop/common/lib/"
cp "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}" "${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME}/jars/"

bash "${dir}/minio/deploy.sh"
bash "${dir}/juicefs/deploy.sh"
bash "${dir}/hadoop/deploy.sh"
bash "${dir}/hms/deploy.sh"
bash "${dir}/flink/deploy.sh"
bash "${dir}/spark/deploy.sh"

cp "${CONF_DIR}"/* "${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/"
cp "${CONF_DIR}"/* "${CLIENT_DIR}/${HIVE_CLIENT_DIR_NAME}/conf/"
cp "${CONF_DIR}"/* "${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME}/conf/"

bash "${dir}/sqlrec/deploy.sh"

# extra components, deploy them if needed
bash "${dir}/kafka/deploy.sh"
bash "${dir}/redis/deploy.sh"
bash "${dir}/milvus/deploy.sh"
#bash "${dir}/hdfs/deploy.sh"
#bash "${dir}/mongodb/deploy_default.sh"
#bash "${dir}/kyuubi/deploy.sh"
#bash "${dir}/jupyter/deploy.sh"
#bash "${dir}/clickhouse/deploy.sh"
#bash "${dir}/growthbook/deploy.sh"
#bash "${dir}/prometheus/deploy.sh"
#bash "${dir}/jaeger/deploy.sh"

echo "deploy components done"
