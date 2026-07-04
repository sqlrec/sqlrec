#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/dolphinscheduler.yaml > ${dir}/dolphinscheduler.yaml.tmp
kubectl delete -f ${dir}/dolphinscheduler.yaml.tmp -n ${NAMESPACE} --ignore-not-found

kubectl delete job dolphinscheduler-init -n ${NAMESPACE} --ignore-not-found
kubectl delete job dolphinscheduler-install-plugins -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap dolphinscheduler-plugins-config -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap dolphinscheduler-env -n ${NAMESPACE} --ignore-not-found
