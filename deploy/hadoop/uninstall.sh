#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

kubectl delete configmap core-site -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap hdfs-site -n ${NAMESPACE} --ignore-not-found
