#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/env.sh"

require_commands kubectl || exit 1
validate_runtime_resources || exit 1

if [ -z "${NODE_IP}" ]; then
  echo "ERROR: NODE_IP is not set; provide a reachable worker address or check for a Ready worker node." >&2
  exit 1
fi

if [ -z "${K8S_APISERVER_ADDR}" ]; then
  echo "ERROR: K8S_APISERVER_ADDR is not set; provide it or check the current kubectl context." >&2
  exit 1
fi

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace "${NAMESPACE}-milvus" --dry-run=client -o yaml | kubectl apply -f -

mkdir -p "${CONF_DIR}"
bash "${dir}/storage/deploy.sh"

# juicefs-hadoop jar must be on the hadoop/spark client classpath before hadoop/deploy.sh runs `hadoop fs` against jfs://
cp "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}" "${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/share/hadoop/common/lib/"
cp "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}" "${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME}/jars/"

bash "${dir}/rustfs/deploy.sh"
bash "${dir}/juicefs/deploy.sh"
bash "${dir}/hadoop/deploy.sh"
bash "${dir}/spark/deploy.sh"
bash "${dir}/hms/deploy.sh"
bash "${dir}/flink/deploy.sh"
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
echo "For Minikube image caching, run: bash ${dir}/cache_images.sh save"
