#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/core-site.xml > ${CONF_DIR}/core-site.xml
envsubst < ${dir}/hdfs-site.xml > ${CONF_DIR}/hdfs-site.xml

cp ${CONF_DIR}/* ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/

hadoop fs -mkdir -p /spark/upload
hadoop fs -mkdir -p /etc
echo "supergroup:0:hdfs,root,${USER}" > groups
hadoop fs -put -f groups /etc
rm groups
sed -i 's/<!--//; s/-->//' ${CONF_DIR}/core-site.xml

cp ${CONF_DIR}/* ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/

kubectl create configmap core-site --from-file="${CONF_DIR}/core-site.xml" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap hdfs-site --from-file="${CONF_DIR}/hdfs-site.xml" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
