#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

mkdir -p "${CONF_DIR}"
cp "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}" "${SPARK_HOME}/jars/"

kubectl create serviceaccount spark -n "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit \
  --serviceaccount="${NAMESPACE}:spark" \
  --dry-run=client -o yaml | kubectl apply -f -

render_config "${dir}/spark-defaults.conf"
cp "${dir}/spark-defaults.conf.tmp" "${CONF_DIR}/spark-defaults.conf"
kubectl create configmap spark-defaults --from-file="spark-defaults.conf=${CONF_DIR}/spark-defaults.conf" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
