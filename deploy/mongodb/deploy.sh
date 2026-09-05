#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

export MONGODB_NAME=$1
export MONGODB_PORT=$2
export MONGODB_USERNAME=$3
export MONGODB_PASSWORD=$4

envsubst < ${dir}/mongo.yaml > ${dir}/mongo.yaml.${MONGODB_NAME}
kubectl apply -f ${dir}/mongo.yaml.${MONGODB_NAME} -n ${NAMESPACE}
kubectl wait --for=condition=Available deployment/${MONGODB_NAME} --timeout=${DEPLOY_TIMEOUT}s -n ${NAMESPACE}
