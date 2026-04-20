#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/core-site.xml > ${CONF_DIR}/core-site.xml
if kubectl get configmap core-site -n "${NAMESPACE}"; then
  kubectl delete configmap core-site -n "${NAMESPACE}"
fi
kubectl create configmap core-site --from-file="${CONF_DIR}/core-site.xml" -n "${NAMESPACE}"

envsubst < ${dir}/hdfs-site.xml > ${CONF_DIR}/hdfs-site.xml
if kubectl get configmap hdfs-site -n "${NAMESPACE}"; then
  kubectl delete configmap hdfs-site -n "${NAMESPACE}"
fi
kubectl create configmap hdfs-site --from-file="${CONF_DIR}/hdfs-site.xml" -n "${NAMESPACE}"

cp ${CONF_DIR}/* ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/
