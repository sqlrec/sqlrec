#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

if [ ! -e ${HDFS_NAMENODE_DATA_DIR} ]; then
  mkdir -p ${HDFS_NAMENODE_DATA_DIR}
  mkdir -p ${HDFS_DATANODE_DATA_DIR}
  echo "export JAVA_HOME=${CLIENT_DIR}/${JAVA_CLIENT_DIR_NAME}" >> ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/hadoop-env.sh
  ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/bin/hdfs namenode -format
fi

envsubst < ${dir}/hdfs.yaml > ${dir}/hdfs.yaml.tmp
kubectl apply -f ${dir}/hdfs.yaml.tmp -n "${NAMESPACE}"
