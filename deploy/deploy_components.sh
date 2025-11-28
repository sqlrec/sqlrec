#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
set -ex
dir=$(dirname $(realpath $0))
source ${dir}/env.sh

# check if NODE_IP is set
if [ -z "${NODE_IP}" ]; then
  echo "NODE_IP is not set"
  exit 1
fi

if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl create namespace "${NAMESPACE}"
fi

if ! kubectl get namespace "${NAMESPACE}-milvus" >/dev/null 2>&1; then
  kubectl create namespace "${NAMESPACE}-milvus"
fi

envsubst < ${dir}/pv.yaml > ${dir}/pv.yaml.tmp
kubectl apply -f ${dir}/pv.yaml.tmp -n ${NAMESPACE}

bash ${dir}/minio/deploy.sh
bash ${dir}/juicefs/deploy.sh
bash ${dir}/kafka/deploy.sh
bash ${dir}/redis/deploy.sh
bash ${dir}/hms/deploy.sh
bash ${dir}/flink/deploy.sh
bash ${dir}/sqlrec/deploy.sh
bash ${dir}/milvus/deploy.sh
bash ${dir}/kyuubi/deploy.sh
bash ${dir}/jupyter/deploy.sh

cp ${CONF_DIR}/* ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/
cp ${CONF_DIR}/* ${CLIENT_DIR}/${HIVE_CLIENT_DIR_NAME}/conf/
cp ${CONF_DIR}/* ${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME}/conf/

cp ${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME} ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/share/hadoop/common/lib/
cp ${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME} ${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME}/jars/

hadoop fs -mkdir -p /spark/upload

echo "deploy components done"