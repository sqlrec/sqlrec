#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

export MONGODB_NAME=$1

if [ -z "${MONGODB_NAME}" ]; then
  echo "Usage: $0 <mongodb_name>"
  exit 1
fi

render_config "${dir}/mongo.yaml"
kubectl delete -f "${dir}/mongo.yaml.tmp" -n ${NAMESPACE} --ignore-not-found
