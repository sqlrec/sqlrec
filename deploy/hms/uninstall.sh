#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/hms.yaml"
kubectl delete -f "${dir}/hms.yaml.tmp" -n ${NAMESPACE} --ignore-not-found

kubectl delete job hms-init -n ${NAMESPACE} --ignore-not-found

kubectl delete configmap hive-site-hms -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap hive-site -n ${NAMESPACE} --ignore-not-found
