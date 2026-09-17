#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

bash "${dir}/../postgresql/deploy.sh" metastore \
  "${HMS_POSTGRESQL_PORT}" "${HMS_POSTGRESQL_USER}" "${HMS_POSTGRESQL_PASSWORD}"

render_config "${dir}/hms.yaml"
render_config "${dir}/hms-init.yaml"
render_config "${dir}/hive-site-hms.xml"
render_config "${dir}/hive-site.xml"

kubectl create configmap hive-site-hms --from-file="hive-site-hms.xml=${dir}/hive-site-hms.xml.tmp" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

kubectl create configmap hive-site --from-file="hive-site.xml=${dir}/hive-site.xml.tmp" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

cp "${dir}/hive-site-hms.xml.tmp" "${CONF_DIR}/hive-site-hms.xml"
cp "${dir}/hive-site.xml.tmp" "${CONF_DIR}/hive-site.xml"

# re-run the init job when it does not exist or did not complete successfully
if ! kubectl get job hms-init -n "${NAMESPACE}" >/dev/null 2>&1 || ! wait_for_job hms-init "${NAMESPACE}" 60; then
  kubectl delete job hms-init -n "${NAMESPACE}" --ignore-not-found
  kubectl apply -f "${dir}/hms-init.yaml.tmp" -n "${NAMESPACE}"
  wait_for_job hms-init "${NAMESPACE}" "${DEPLOY_TIMEOUT}"
fi

kubectl apply -f "${dir}/hms.yaml.tmp" -n "${NAMESPACE}"
