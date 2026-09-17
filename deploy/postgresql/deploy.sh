#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <database> <node-port> <owner> <password>" >&2
  exit 2
fi

export POSTGRESQL_DB="$1"
export POSTGRESQL_PORT="$2"
export POSTGRESQL_OWNER="$3"
export POSTGRESQL_PASSWORD="$4"
export POSTGRESQL_OWNER_BASE64="$(printf '%s' "${POSTGRESQL_OWNER}" | base64)"
export POSTGRESQL_PASSWORD_BASE64="$(printf '%s' "${POSTGRESQL_PASSWORD}" | base64)"

envsubst < "${dir}/pg.yaml" > "${dir}/pg.${POSTGRESQL_DB}.yaml.tmp"
kubectl apply -f "${dir}/pg.${POSTGRESQL_DB}.yaml.tmp" -n "${NAMESPACE}"
kubectl wait --for=condition=Ready "cluster/${POSTGRESQL_DB}-postgresql" --timeout="${DEPLOY_TIMEOUT}s" -n "${NAMESPACE}"
