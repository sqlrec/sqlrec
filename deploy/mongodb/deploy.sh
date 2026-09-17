#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <name> <node-port> <username> <password>" >&2
  exit 2
fi

export MONGODB_NAME="$1"
export MONGODB_PORT="$2"
export MONGODB_USERNAME="$3"
export MONGODB_PASSWORD="$4"

render_config "${dir}/mongo.yaml"
kubectl apply -f "${dir}/mongo.yaml.tmp" -n "${NAMESPACE}"
kubectl wait --for=condition=Available "deployment/${MONGODB_NAME}" --timeout="${DEPLOY_TIMEOUT}s" -n "${NAMESPACE}"
