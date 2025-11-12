#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

envsubst < ${dir}/sql_gateway.yaml > ${dir}/sql_gateway.yaml.tmp
kubectl apply -f "${dir}/sql_gateway.yaml.tmp" -n "${NAMESPACE}"
