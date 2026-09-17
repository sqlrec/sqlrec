#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

export DOLPHINSCHEDULER_DB="dolphinscheduler"

render_config "${dir}/dolphinscheduler.yaml"
kubectl delete -f "${dir}/dolphinscheduler.yaml.tmp" -n ${NAMESPACE} --ignore-not-found

kubectl delete job dolphinscheduler-init -n ${NAMESPACE} --ignore-not-found
kubectl delete job dolphinscheduler-install-plugins -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap dolphinscheduler-plugins-config -n ${NAMESPACE} --ignore-not-found
kubectl delete configmap dolphinscheduler-env -n ${NAMESPACE} --ignore-not-found
