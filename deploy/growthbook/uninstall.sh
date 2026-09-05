#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

export GROWTHBOOK_NAME=growthbook
export GROWTHBOOK_MONGODB_NAME=${GROWTHBOOK_NAME}-mongodb

bash ${dir}/../mongodb/uninstall.sh ${GROWTHBOOK_MONGODB_NAME} ${GROWTHBOOK_MONGODB_PORT} ${GROWTHBOOK_MONGODB_USER} ${GROWTHBOOK_MONGODB_PASSWORD}

envsubst < ${dir}/growthbook.yaml > ${dir}/growthbook.yaml.${GROWTHBOOK_NAME}
kubectl delete -f ${dir}/growthbook.yaml.${GROWTHBOOK_NAME} -n ${NAMESPACE} --ignore-not-found
