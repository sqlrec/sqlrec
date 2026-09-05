#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/hdfs.yaml"
kubectl delete -f "${dir}/hdfs.yaml.tmp" -n ${NAMESPACE} --ignore-not-found

kubectl delete job hdfs-namenode-init -n ${NAMESPACE} --ignore-not-found

render_config "${dir}/hdfs-pvc.yaml"
kubectl delete -f "${dir}/hdfs-pvc.yaml.tmp" -n ${NAMESPACE} --ignore-not-found
