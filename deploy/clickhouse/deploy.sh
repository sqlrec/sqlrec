#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/clickhouse.yaml > ${dir}/clickhouse.yaml.rendered
kubectl apply -f ${dir}/clickhouse.yaml.rendered -n ${NAMESPACE}
kubectl wait --for=condition=Ready pod -l app=clickhouse --timeout=3600s -n ${NAMESPACE}
