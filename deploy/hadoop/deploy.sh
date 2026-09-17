#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/core-site.xml"
render_config "${dir}/hdfs-site.xml"

cp "${dir}/core-site.xml.tmp" "${CONF_DIR}/core-site.xml"
cp "${dir}/hdfs-site.xml.tmp" "${CONF_DIR}/hdfs-site.xml"

cp "${CONF_DIR}"/* "${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/"

hadoop fs -mkdir -p /spark/upload
hadoop fs -mkdir -p /etc
groups_file="$(mktemp "${TMPDIR:-/tmp}/sqlrec-groups.XXXXXX")"
trap 'rm -f "${groups_file}"' EXIT INT TERM
printf 'supergroup:0:hdfs,root,%s\n' "${USER}" > "${groups_file}"
hadoop fs -put -f "${groups_file}" /etc/groups
rm -f "${groups_file}"
trap - EXIT INT TERM
sed 's/<!--//; s/-->//' "${dir}/core-site.xml.tmp" > "${dir}/core-site.rendering.xml.tmp"
mv "${dir}/core-site.rendering.xml.tmp" "${dir}/core-site.xml.tmp"
cp "${dir}/core-site.xml.tmp" "${CONF_DIR}/core-site.xml"

cp "${CONF_DIR}"/* "${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME}/etc/hadoop/"

kubectl create configmap core-site --from-file="core-site.xml=${dir}/core-site.xml.tmp" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap hdfs-site --from-file="hdfs-site.xml=${dir}/hdfs-site.xml.tmp" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
