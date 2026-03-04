#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

# refer to https://strimzi.io/quickstarts
envsubst < ${dir}/kafka.yaml > ${dir}/kafka.yaml.tmp
kubectl apply -f "${dir}/kafka.yaml.tmp" -n "${NAMESPACE}"
