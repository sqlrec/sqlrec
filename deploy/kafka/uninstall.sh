#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/kafka.yaml"
kubectl delete -f "${dir}/kafka.yaml.tmp" -n ${NAMESPACE} --ignore-not-found
