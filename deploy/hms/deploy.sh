#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

helm upgrade --install mysql-hms \
  --namespace "${NAMESPACE}" \
  --set primary.service.type=NodePort \
  --set primary.service.nodePorts.mysql=${HMS_MYSQL_PORT} \
  --set auth.database=metastore,auth.username=${HMS_MYSQL_USER},auth.password=${HMS_MYSQL_PASSWORD} \
  --wait \
  --timeout 3600s \
  oci://registry-1.docker.io/bitnamicharts/mysql

envsubst < ${dir}/hms.yaml > ${dir}/hms.yaml.tmp
envsubst < ${dir}/hms-init.yaml > ${dir}/hms-init.yaml.tmp
envsubst < ${dir}/hive-site-hms.template > ${CONF_DIR}/hive-site-hms.xml
envsubst < ${dir}/hive-site.template > ${CONF_DIR}/hive-site.xml

kubectl create configmap hive-site-hms --from-file="${CONF_DIR}/hive-site-hms.xml" -n "${NAMESPACE}"
kubectl create configmap hive-site --from-file="${CONF_DIR}/hive-site.xml" -n "${NAMESPACE}"

kubectl apply -f "${dir}/hms-init.yaml.tmp" -n "${NAMESPACE}"
sleep 15

kubectl apply -f "${dir}/hms.yaml.tmp" -n "${NAMESPACE}"
