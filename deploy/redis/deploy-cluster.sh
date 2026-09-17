#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

# Only substitute the env.sh variables; shell variables inside container
# commands (e.g. ${POD_NAME}, ${PORT}) must be preserved for runtime.
ENVSUBST_VARS='${NAMESPACE} ${VALKEY_VERSION} ${NODE_IP} ${REDIS_CLUSTER_BASE_PORT} ${REDIS_CLUSTER_NODES}'
render_config "${dir}/redis-cluster.yaml" "${ENVSUBST_VARS}"

# Delete the init job if it already exists (completed jobs are immutable and won't re-run on apply)
kubectl delete job redis-cluster-init -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true

kubectl apply -f "${dir}/redis-cluster.yaml.tmp" -n "${NAMESPACE}"

# Wait for the cluster init job to complete
wait_for_job redis-cluster-init "${NAMESPACE}"
