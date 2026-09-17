#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

NAMENODE_PVC_MISSING=false
DATANODE_PVC_MISSING=false
if ! kubectl get pvc "${HDFS_NAMENODE_PVC_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  NAMENODE_PVC_MISSING=true
fi
if ! kubectl get pvc "${HDFS_DATANODE_PVC_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  DATANODE_PVC_MISSING=true
fi

if [ "${NAMENODE_PVC_MISSING}" = true ] || [ "${DATANODE_PVC_MISSING}" = true ]; then
  render_config "${dir}/hdfs-pvc.yaml"
  kubectl apply -f "${dir}/hdfs-pvc.yaml.tmp" -n "${NAMESPACE}"
fi

# Re-run the init job when it does not exist or did not complete successfully.
# PVC existence alone is not enough: the PVC may have survived a failed init job.
if [ "${NAMENODE_PVC_MISSING}" = true ]; then
  # A new Namenode PVC cannot use the old Job's successful status.
  kubectl delete job hdfs-namenode-init -n "${NAMESPACE}" --ignore-not-found
fi
if ! kubectl get job hdfs-namenode-init -n "${NAMESPACE}" >/dev/null 2>&1 || \
   ! wait_for_job hdfs-namenode-init "${NAMESPACE}" 60; then
  kubectl delete job hdfs-namenode-init -n "${NAMESPACE}" --ignore-not-found

  render_config "${dir}/hdfs-init-job.yaml"
  kubectl apply -f "${dir}/hdfs-init-job.yaml.tmp" -n "${NAMESPACE}"
  wait_for_job hdfs-namenode-init "${NAMESPACE}" "${DEPLOY_TIMEOUT}"
fi

render_config "${dir}/hdfs.yaml"
kubectl apply -f "${dir}/hdfs.yaml.tmp" -n "${NAMESPACE}"
