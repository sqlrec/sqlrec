#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

envsubst < ${dir}/hms.yaml > ${dir}/hms.yaml.tmp
kubectl delete -f ${dir}/hms.yaml.tmp -n ${NAMESPACE} --ignore-not-found

kubectl delete job hms-init -n ${NAMESPACE} --ignore-not-found

kubectl delete configmap hive-site-hms -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap hive-site -n ${NAMESPACE} --ignore-not-found
