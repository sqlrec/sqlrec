#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

kubectl delete serviceaccount spark -n ${NAMESPACE} --ignore-not-found
kubectl delete clusterrolebinding spark-role --ignore-not-found
kubectl delete configmap spark-defaults -n ${NAMESPACE} --ignore-not-found
