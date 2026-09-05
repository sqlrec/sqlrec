#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

bash ${dir}/../postgresql/deploy.sh metastore ${HMS_POSTGRESQL_PORT} ${HMS_POSTGRESQL_USER} ${HMS_POSTGRESQL_PASSWORD}

envsubst < ${dir}/hms.yaml > ${dir}/hms.yaml.tmp
envsubst < ${dir}/hms-init.yaml > ${dir}/hms-init.yaml.tmp
envsubst < ${dir}/hive-site-hms.template > ${CONF_DIR}/hive-site-hms.xml
envsubst < ${dir}/hive-site.template > ${CONF_DIR}/hive-site.xml

kubectl create configmap hive-site-hms --from-file="${CONF_DIR}/hive-site-hms.xml" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap hive-site --from-file="${CONF_DIR}/hive-site.xml" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# re-run the init job when it does not exist or did not complete successfully
if ! kubectl get job hms-init -n "${NAMESPACE}" >/dev/null 2>&1 || ! wait_for_job hms-init "${NAMESPACE}" 60; then
  kubectl delete job hms-init -n "${NAMESPACE}" --ignore-not-found
  kubectl apply -f "${dir}/hms-init.yaml.tmp" -n "${NAMESPACE}"
  wait_for_job hms-init "${NAMESPACE}" "${DEPLOY_TIMEOUT}"
fi

kubectl apply -f "${dir}/hms.yaml.tmp" -n "${NAMESPACE}"
