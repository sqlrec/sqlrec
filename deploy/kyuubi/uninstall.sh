#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

envsubst < ${dir}/kyuubi.yaml > ${dir}/kyuubi.yaml.tmp
kubectl delete -f "${dir}/kyuubi.yaml.tmp" -n "${NAMESPACE}" --ignore-not-found
rm -f "${dir}/kyuubi.yaml.tmp"
