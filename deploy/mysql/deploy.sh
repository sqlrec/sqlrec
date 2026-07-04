#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/mysql.yaml > ${dir}/mysql.yaml.${MYSQL_NAME}
kubectl apply -f ${dir}/mysql.yaml.${MYSQL_NAME} -n ${NAMESPACE}
kubectl wait --for=condition=Ready pod -l app=${MYSQL_NAME} --timeout=3600s -n ${NAMESPACE}
