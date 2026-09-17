#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/mysql.yaml"
kubectl apply -f "${dir}/mysql.yaml.tmp" -n ${NAMESPACE}
kubectl wait --for=condition=Available deployment/${MYSQL_NAME} --timeout=${DEPLOY_TIMEOUT}s -n ${NAMESPACE}
