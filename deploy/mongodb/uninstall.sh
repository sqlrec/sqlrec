#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

export MONGODB_NAME=$1

if [ -z "${MONGODB_NAME}" ]; then
  echo "Usage: $0 <mongodb_name>"
  exit 1
fi

envsubst < ${dir}/mongo.yaml > ${dir}/mongo.yaml.${MONGODB_NAME}
kubectl delete -f ${dir}/mongo.yaml.${MONGODB_NAME} -n ${NAMESPACE} --ignore-not-found
