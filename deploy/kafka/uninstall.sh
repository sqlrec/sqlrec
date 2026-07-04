#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/kafka.yaml > ${dir}/kafka.yaml.tmp
kubectl delete -f ${dir}/kafka.yaml.tmp -n ${NAMESPACE} --ignore-not-found
