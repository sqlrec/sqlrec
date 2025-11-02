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

cp ${CONF_DIR}/* ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/
cp ${CONF_DIR}/* ${CLIENT_DIR}/${HIVE_CLIENT_DIR_NAME}/conf/
cp ${CONF_DIR}/* ${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME}/conf/

export HADOOP_HOME=${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}
export HIVE_HOME=${CLIENT_DIR}/${HIVE_CLIENT_DIR_NAME}
export SPARK_HOME=${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME}
export PATH=${PATH}:${HADOOP_HOME}/bin:${SPARK_HOME}/bin:${HIVE_HOME}/bin

hadoop fs -mkdir -p /spark/upload
