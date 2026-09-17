#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

kubectl create serviceaccount spark -n "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit \
  --serviceaccount="${NAMESPACE}:spark" \
  --dry-run=client -o yaml | kubectl apply -f -

render_config "${dir}/spark-defaults.conf"
kubectl create configmap spark-defaults --from-file="spark-defaults.conf=${dir}/spark-defaults.conf.tmp" -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
cp "${dir}/spark-defaults.conf.tmp" "${CONF_DIR}/spark-defaults.conf"
