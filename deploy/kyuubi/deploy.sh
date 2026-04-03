#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/kyuubi.yaml > ${dir}/kyuubi.yaml.tmp
kubectl apply -f "${dir}/kyuubi.yaml.tmp" -n "${NAMESPACE}"
