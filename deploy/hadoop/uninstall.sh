#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

kubectl delete configmap core-site -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap hdfs-site -n ${NAMESPACE} --ignore-not-found
