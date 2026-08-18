#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

ENVSUBST_VARS='${NAMESPACE} ${VALKEY_VERSION} ${NODE_IP} ${HOST_IP} ${REDIS_CLUSTER_BASE_PORT} ${REDIS_CLUSTER_NODES}'
envsubst "${ENVSUBST_VARS}" \
  < ${dir}/redis-cluster.yaml > ${dir}/redis-cluster.yaml.tmp
kubectl delete -f "${dir}/redis-cluster.yaml.tmp" -n "${NAMESPACE}" --ignore-not-found
rm -f ${dir}/redis-cluster.yaml.tmp

# Clean up PVCs created by the StatefulSet
for i in $(seq 0 $((${REDIS_CLUSTER_NODES} - 1))); do
  kubectl delete pvc -n "${NAMESPACE}" "data-redis-cluster-${i}" --ignore-not-found 2>/dev/null || true
done
