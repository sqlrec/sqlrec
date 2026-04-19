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

envsubst < ${dir}/dolphinscheduler.yaml > ${dir}/dolphinscheduler.yaml.tmp
kubectl apply -f "${dir}/dolphinscheduler.yaml.tmp" -n "${NAMESPACE}"

# need install plugin in pod, refer https://dolphinscheduler.apache.org/zh-cn/docs/3.4.1/guide/installation/standalone
