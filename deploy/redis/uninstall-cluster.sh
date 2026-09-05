#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

ENVSUBST_VARS='${NAMESPACE} ${VALKEY_VERSION} ${NODE_IP} ${REDIS_CLUSTER_BASE_PORT} ${REDIS_CLUSTER_NODES}'
render_config "${dir}/redis-cluster.yaml" "${ENVSUBST_VARS}"
kubectl delete -f "${dir}/redis-cluster.yaml.tmp" -n "${NAMESPACE}" --ignore-not-found

# Clean up PVCs created by the StatefulSet
for i in $(seq 0 $((${REDIS_CLUSTER_NODES} - 1))); do
  kubectl delete pvc -n "${NAMESPACE}" "data-redis-cluster-${i}" --ignore-not-found 2>/dev/null || true
done
