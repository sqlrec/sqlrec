#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/clickhouse.yaml"
kubectl apply -f "${dir}/clickhouse.yaml.tmp" -n ${NAMESPACE}
kubectl wait --for=condition=Available deployment/clickhouse --timeout=${DEPLOY_TIMEOUT}s -n ${NAMESPACE}
