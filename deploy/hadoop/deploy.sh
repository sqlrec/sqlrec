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

if [ "${DEPLOY_HDFS,,}" != "true" ]; then
  echo "HDFS deployment is disabled (DEPLOY_HDFS=${DEPLOY_HDFS}), exiting."
  exit 0
fi

if [ ! -e ${HDFS_NAMENODE_DATA_DIR} ]; then
  mkdir -p ${HDFS_NAMENODE_DATA_DIR}
  mkdir -p ${HDFS_DATANODE_DATA_DIR}
  echo "export JAVA_HOME=${CLIENT_DIR}/${JAVA_CLIENT_DIR_NAME}" >> ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/hadoop-env.sh
  ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/bin/hdfs namenode -format
fi

envsubst < ${dir}/hdfs.yaml > ${dir}/hdfs.yaml.tmp
kubectl apply -f ${dir}/hdfs.yaml.tmp -n "${NAMESPACE}"
