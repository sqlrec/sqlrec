#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

export DOLPHINSCHEDULER_DB="dolphinscheduler"

envsubst < ${dir}/dolphinscheduler.yaml > ${dir}/dolphinscheduler.yaml.tmp
kubectl delete -f ${dir}/dolphinscheduler.yaml.tmp -n ${NAMESPACE} --ignore-not-found

kubectl delete job dolphinscheduler-init -n ${NAMESPACE} --ignore-not-found
kubectl delete job dolphinscheduler-install-plugins -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap dolphinscheduler-plugins-config -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap dolphinscheduler-env -n ${NAMESPACE} --ignore-not-found
