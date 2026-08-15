#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

export DOLPHINSCHEDULER_DB="dolphinscheduler"

bash ${dir}/../postgresql/deploy.sh ${DOLPHINSCHEDULER_DB} ${DOLPHINSCHEDULER_POSTGRESQL_PORT} ${DOLPHINSCHEDULER_POSTGRESQL_USER} ${DOLPHINSCHEDULER_POSTGRESQL_PASSWORD}

envsubst < ${dir}/dolphinscheduler-init.yaml > ${dir}/dolphinscheduler-init.yaml.tmp

kubectl apply -f "${dir}/dolphinscheduler-init.yaml.tmp" -n "${NAMESPACE}"
wait_for_job dolphinscheduler-init "${NAMESPACE}" "${DEPLOY_TIMEOUT}"

kubectl create configmap dolphinscheduler-plugins-config --from-file="${dir}/plugins_config" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

envsubst < ${dir}/dolphinscheduler_env.sh.template > ${CONF_DIR}/dolphinscheduler_env.sh
kubectl create configmap dolphinscheduler-env --from-file="${CONF_DIR}/dolphinscheduler_env.sh" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

envsubst < ${dir}/dolphinscheduler-install-plugins.yaml > ${dir}/dolphinscheduler-install-plugins.yaml.tmp
kubectl apply -f "${dir}/dolphinscheduler-install-plugins.yaml.tmp" -n "${NAMESPACE}"
wait_for_job dolphinscheduler-install-plugins "${NAMESPACE}" "${DEPLOY_TIMEOUT}"

envsubst < ${dir}/dolphinscheduler.yaml > ${dir}/dolphinscheduler.yaml.tmp
kubectl apply -f "${dir}/dolphinscheduler.yaml.tmp" -n "${NAMESPACE}"

# refer https://dolphinscheduler.apache.org/zh-cn/docs/3.4.1/guide/installation/standalone
echo "login in with http://${NODE_IP}:${DOLPHINSCHEDULER_PORT}/dolphinscheduler/ui"
echo "default: admin/dolphinscheduler123"
