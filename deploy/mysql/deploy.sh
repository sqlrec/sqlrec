#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

envsubst < ${dir}/mysql.yaml > ${dir}/mysql.yaml.${MYSQL_NAME}
kubectl apply -f ${dir}/mysql.yaml.${MYSQL_NAME} -n ${NAMESPACE}
kubectl wait --for=condition=Available deployment/${MYSQL_NAME} --timeout=${DEPLOY_TIMEOUT}s -n ${NAMESPACE}
