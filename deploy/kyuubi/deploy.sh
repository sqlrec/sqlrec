#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

envsubst < ${dir}/kyuubi.yaml > ${dir}/kyuubi.yaml.tmp
kubectl apply -f "${dir}/kyuubi.yaml.tmp" -n "${NAMESPACE}"
kubectl rollout status deployment/kyuubi -n "${NAMESPACE}"
