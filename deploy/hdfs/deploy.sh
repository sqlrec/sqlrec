#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

if ! kubectl get pvc ${HDFS_NAMENODE_PVC_NAME} -n "${NAMESPACE}" >/dev/null 2>&1; then
  envsubst < ${dir}/hdfs-pvc.yaml > ${dir}/hdfs-pvc.yaml.tmp
  kubectl apply -f ${dir}/hdfs-pvc.yaml.tmp -n "${NAMESPACE}"
  
  envsubst < ${dir}/hdfs-init-job.yaml > ${dir}/hdfs-init-job.yaml.tmp
  kubectl apply -f ${dir}/hdfs-init-job.yaml.tmp -n "${NAMESPACE}"
  
  wait_for_job hdfs-namenode-init "${NAMESPACE}" "${DEPLOY_TIMEOUT}"
fi

envsubst < ${dir}/hdfs.yaml > ${dir}/hdfs.yaml.tmp
kubectl apply -f ${dir}/hdfs.yaml.tmp -n "${NAMESPACE}"
