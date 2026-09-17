#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/clickhouse.yaml"
kubectl delete -f "${dir}/clickhouse.yaml.tmp" -n ${NAMESPACE} --ignore-not-found
