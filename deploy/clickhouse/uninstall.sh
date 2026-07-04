#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/clickhouse.yaml > ${dir}/clickhouse.yaml.rendered
kubectl delete -f ${dir}/clickhouse.yaml.rendered -n ${NAMESPACE} --ignore-not-found
