#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/mysql.yaml > ${dir}/mysql.yaml.${MYSQL_NAME}
kubectl delete -f ${dir}/mysql.yaml.${MYSQL_NAME} -n ${NAMESPACE} --ignore-not-found
