#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/sql_gateway.yaml"
kubectl delete -f "${dir}/sql_gateway.yaml.tmp" -n ${NAMESPACE} --ignore-not-found
