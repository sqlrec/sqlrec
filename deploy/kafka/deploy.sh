#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

# refer to https://strimzi.io/quickstarts
envsubst < ${dir}/kafka.yaml > ${dir}/kafka.yaml.tmp
kubectl apply -f "${dir}/kafka.yaml.tmp" -n "${NAMESPACE}"
