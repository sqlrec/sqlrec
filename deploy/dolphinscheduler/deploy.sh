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
kubectl wait --for=condition=complete job/dolphinscheduler-init --timeout=3600s -n "${NAMESPACE}"

if kubectl get configmap dolphinscheduler-plugins-config -n "${NAMESPACE}"; then
  kubectl delete configmap dolphinscheduler-plugins-config -n "${NAMESPACE}"
fi
kubectl create configmap dolphinscheduler-plugins-config --from-file="${dir}/plugins_config" -n "${NAMESPACE}"

if kubectl get configmap dolphinscheduler-env -n "${NAMESPACE}"; then
  kubectl delete configmap dolphinscheduler-env -n "${NAMESPACE}"
fi
envsubst < ${dir}/dolphinscheduler_env.sh.template > ${dir}/dolphinscheduler_env.sh
kubectl create configmap dolphinscheduler-env --from-file="${dir}/dolphinscheduler_env.sh" -n "${NAMESPACE}"

envsubst < ${dir}/dolphinscheduler-install-plugins.yaml > ${dir}/dolphinscheduler-install-plugins.yaml.tmp
kubectl apply -f "${dir}/dolphinscheduler-install-plugins.yaml.tmp" -n "${NAMESPACE}"
kubectl wait --for=condition=complete job/dolphinscheduler-install-plugins --timeout=3600s -n "${NAMESPACE}"

envsubst < ${dir}/dolphinscheduler.yaml > ${dir}/dolphinscheduler.yaml.tmp
kubectl apply -f "${dir}/dolphinscheduler.yaml.tmp" -n "${NAMESPACE}"

# refer https://dolphinscheduler.apache.org/zh-cn/docs/3.4.1/guide/installation/standalone
echo "login in with http://${NODE_IP}:${DOLPHINSCHEDULER_PORT}/dolphinscheduler/ui"
echo "default: admin/dolphinscheduler123"
