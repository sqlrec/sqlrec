#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

helm upgrade --install postgresql-hms \
  --namespace "${NAMESPACE}" \
  --set primary.service.type=NodePort \
  --set primary.service.nodePorts.postgresql=${HMS_POSTGRESQL_PORT} \
  --set auth.database=metastore,auth.username=${HMS_POSTGRESQL_USER},auth.password=${HMS_POSTGRESQL_PASSWORD} \
  --set image.repository=bitnamilegacy/postgresql \
  --wait \
  --timeout 3600s \
  oci://registry-1.docker.io/bitnamicharts/postgresql

envsubst < ${dir}/hms.yaml > ${dir}/hms.yaml.tmp
envsubst < ${dir}/hms-init.yaml > ${dir}/hms-init.yaml.tmp
envsubst < ${dir}/hive-site-hms.template > ${CONF_DIR}/hive-site-hms.xml
envsubst < ${dir}/hive-site.template > ${CONF_DIR}/hive-site.xml

if kubectl get configmap hive-site-hms -n "${NAMESPACE}"; then
  kubectl delete configmap hive-site-hms -n "${NAMESPACE}"
fi
kubectl create configmap hive-site-hms --from-file="${CONF_DIR}/hive-site-hms.xml" -n "${NAMESPACE}"

if kubectl get configmap hive-site -n "${NAMESPACE}"; then
  kubectl delete configmap hive-site -n "${NAMESPACE}"
fi
kubectl create configmap hive-site --from-file="${CONF_DIR}/hive-site.xml" -n "${NAMESPACE}"

kubectl apply -f "${dir}/hms-init.yaml.tmp" -n "${NAMESPACE}"
sleep 15

kubectl apply -f "${dir}/hms.yaml.tmp" -n "${NAMESPACE}"
