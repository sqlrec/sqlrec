#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

bash "${dir}/init.sh"

render_config "${dir}/kyuubi.yaml"
kubectl apply -f "${dir}/kyuubi.yaml.tmp" -n "${NAMESPACE}"
kubectl rollout status deployment/kyuubi -n "${NAMESPACE}" --timeout="${DEPLOY_TIMEOUT}s"
