#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

NAMENODE_PVC_MISSING=false
DATANODE_PVC_MISSING=false
if ! kubectl get pvc "${HDFS_NAMENODE_PVC_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  NAMENODE_PVC_MISSING=true
fi
if ! kubectl get pvc "${HDFS_DATANODE_PVC_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  DATANODE_PVC_MISSING=true
fi

if [ "${NAMENODE_PVC_MISSING}" = true ] || [ "${DATANODE_PVC_MISSING}" = true ]; then
  envsubst < ${dir}/hdfs-pvc.yaml > ${dir}/hdfs-pvc.yaml.tmp
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

  envsubst < ${dir}/hdfs-init-job.yaml > ${dir}/hdfs-init-job.yaml.tmp
  kubectl apply -f "${dir}/hdfs-init-job.yaml.tmp" -n "${NAMESPACE}"
  wait_for_job hdfs-namenode-init "${NAMESPACE}" "${DEPLOY_TIMEOUT}"
fi

envsubst < ${dir}/hdfs.yaml > ${dir}/hdfs.yaml.tmp
kubectl apply -f "${dir}/hdfs.yaml.tmp" -n "${NAMESPACE}"
