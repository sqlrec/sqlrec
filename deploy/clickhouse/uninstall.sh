#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

envsubst < ${dir}/clickhouse.yaml > ${dir}/clickhouse.yaml.rendered
kubectl delete -f ${dir}/clickhouse.yaml.rendered -n ${NAMESPACE} --ignore-not-found
