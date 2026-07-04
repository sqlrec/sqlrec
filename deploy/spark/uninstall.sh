#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

kubectl delete serviceaccount spark -n ${NAMESPACE} --ignore-not-found
kubectl delete clusterrolebinding spark-role --ignore-not-found
kubectl delete configmap spark-defaults -n ${NAMESPACE} --ignore-not-found
