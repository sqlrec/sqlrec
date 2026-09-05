#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/sql_gateway.yaml"
kubectl apply -f "${dir}/sql_gateway.yaml.tmp" -n "${NAMESPACE}"
