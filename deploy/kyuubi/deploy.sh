#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

if ! kubectl get serviceaccount spark -n "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl create serviceaccount spark -n "${NAMESPACE}"
  kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount="${NAMESPACE}":spark --namespace="${NAMESPACE}"
fi

envsubst < ${dir}/spark-defaults.conf.template > ${CONF_DIR}/spark-defaults.conf
if kubectl get configmap spark-defaults -n "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl delete configmap spark-defaults -n "${NAMESPACE}"
fi
kubectl create configmap spark-defaults --from-file="${CONF_DIR}/spark-defaults.conf" -n "${NAMESPACE}"

envsubst < ${dir}/kyuubi.yaml > ${dir}/kyuubi.yaml.tmp
kubectl apply -f "${dir}/kyuubi.yaml.tmp" -n "${NAMESPACE}"
