#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/core-site.xml"
render_config "${dir}/hdfs-site.xml"

cp "${dir}/core-site.xml.tmp" "${CONF_DIR}/core-site.xml"
cp "${dir}/hdfs-site.xml.tmp" "${CONF_DIR}/hdfs-site.xml"

cp "${CONF_DIR}"/* "${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/"

hadoop fs -mkdir -p /spark/upload
hadoop fs -mkdir -p /etc
echo "supergroup:0:hdfs,root,${USER}" > groups
hadoop fs -put -f groups /etc
rm groups
sed 's/<!--//; s/-->//' "${dir}/core-site.xml.tmp" > "${dir}/core-site.rendering.xml.tmp"
mv "${dir}/core-site.rendering.xml.tmp" "${dir}/core-site.xml.tmp"
cp "${dir}/core-site.xml.tmp" "${CONF_DIR}/core-site.xml"

cp "${CONF_DIR}"/* "${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/"

kubectl create configmap core-site --from-file="core-site.xml=${dir}/core-site.xml.tmp" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap hdfs-site --from-file="hdfs-site.xml=${dir}/hdfs-site.xml.tmp" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
