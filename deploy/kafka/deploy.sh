#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

# refer to https://strimzi.io/quickstarts
envsubst < ${dir}/kafka.yaml > ${dir}/kafka.yaml.tmp
kubectl apply -f "${dir}/kafka.yaml.tmp" -n "${NAMESPACE}"
