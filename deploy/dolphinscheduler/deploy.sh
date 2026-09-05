#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

export DOLPHINSCHEDULER_DB="dolphinscheduler"

bash "${dir}/../postgresql/deploy.sh" ${DOLPHINSCHEDULER_DB} ${DOLPHINSCHEDULER_POSTGRESQL_PORT} ${DOLPHINSCHEDULER_POSTGRESQL_USER} ${DOLPHINSCHEDULER_POSTGRESQL_PASSWORD}

render_config "${dir}/dolphinscheduler-init.yaml"

kubectl apply -f "${dir}/dolphinscheduler-init.yaml.tmp" -n "${NAMESPACE}"
wait_for_job dolphinscheduler-init "${NAMESPACE}" "${DEPLOY_TIMEOUT}"

kubectl create configmap dolphinscheduler-plugins-config --from-file="${dir}/plugins_config" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

render_config "${dir}/dolphinscheduler_env.sh"
kubectl create configmap dolphinscheduler-env --from-file="dolphinscheduler_env.sh=${dir}/dolphinscheduler_env.sh.tmp" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

render_config "${dir}/dolphinscheduler-install-plugins.yaml"
kubectl apply -f "${dir}/dolphinscheduler-install-plugins.yaml.tmp" -n "${NAMESPACE}"
wait_for_job dolphinscheduler-install-plugins "${NAMESPACE}" "${DEPLOY_TIMEOUT}"

render_config "${dir}/dolphinscheduler.yaml"
kubectl apply -f "${dir}/dolphinscheduler.yaml.tmp" -n "${NAMESPACE}"

# refer https://dolphinscheduler.apache.org/zh-cn/docs/3.4.1/guide/installation/standalone
echo "login in with http://${NODE_IP}:${DOLPHINSCHEDULER_PORT}/dolphinscheduler/ui"
echo "default: admin/dolphinscheduler123"
