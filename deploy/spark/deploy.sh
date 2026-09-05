#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

if ! kubectl get serviceaccount spark -n "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl create serviceaccount spark -n "${NAMESPACE}"
  kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount="${NAMESPACE}":spark --namespace="${NAMESPACE}"
fi

render_config "${dir}/spark-defaults.conf"
kubectl create configmap spark-defaults --from-file="spark-defaults.conf=${dir}/spark-defaults.conf.tmp" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
cp "${dir}/spark-defaults.conf.tmp" "${CONF_DIR}/spark-defaults.conf"
