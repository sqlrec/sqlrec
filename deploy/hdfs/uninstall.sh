#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

envsubst < ${dir}/hdfs.yaml > ${dir}/hdfs.yaml.tmp
kubectl delete -f ${dir}/hdfs.yaml.tmp -n ${NAMESPACE} --ignore-not-found

kubectl delete job hdfs-namenode-init -n ${NAMESPACE} --ignore-not-found

envsubst < ${dir}/hdfs-pvc.yaml > ${dir}/hdfs-pvc.yaml.tmp
kubectl delete -f ${dir}/hdfs-pvc.yaml.tmp -n ${NAMESPACE} --ignore-not-found
