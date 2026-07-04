#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

helm uninstall prometheus -n ${NAMESPACE}

kubectl delete servicemonitor sqlrec-servicemonitor -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap sqlrec-jvm-dashboard -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap sqlrec-dashboard -n ${NAMESPACE} --ignore-not-found
