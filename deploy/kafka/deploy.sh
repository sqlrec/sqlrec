#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

# refer to https://strimzi.io/quickstarts
render_config "${dir}/kafka.yaml"
kubectl apply -f "${dir}/kafka.yaml.tmp" -n "${NAMESPACE}"
