#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/sqlrec.yaml > ${dir}/sqlrec.yaml.tmp
kubectl delete -f ${dir}/sqlrec.yaml.tmp -n ${NAMESPACE} --ignore-not-found

kubectl delete serviceaccount sqlrec -n ${NAMESPACE} --ignore-not-found
kubectl delete clusterrolebinding sqlrec-role --ignore-not-found
