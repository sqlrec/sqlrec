#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

helm uninstall prometheus -n ${NAMESPACE}

kubectl delete servicemonitor sqlrec-servicemonitor -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap sqlrec-jvm-dashboard -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap sqlrec-dashboard -n ${NAMESPACE} --ignore-not-found
