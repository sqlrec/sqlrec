#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/sql_gateway.yaml > ${dir}/sql_gateway.yaml.tmp
kubectl apply -f "${dir}/sql_gateway.yaml.tmp" -n "${NAMESPACE}"
