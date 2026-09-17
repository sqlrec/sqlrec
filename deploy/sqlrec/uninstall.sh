#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/sqlrec.yaml"
kubectl delete -f "${dir}/sqlrec.yaml.tmp" -n ${NAMESPACE} --ignore-not-found

kubectl delete serviceaccount sqlrec -n ${NAMESPACE} --ignore-not-found
kubectl delete clusterrolebinding sqlrec-role --ignore-not-found
