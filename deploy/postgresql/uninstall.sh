#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

export POSTGRESQL_DB=$1

if [ -z "${POSTGRESQL_DB}" ]; then
  echo "Usage: $0 <postgresql_db>"
  exit 1
fi

export POSTGRESQL_PORT=$2
export POSTGRESQL_OWNER=$3
export POSTGRESQL_PASSWORD=$4
export POSTGRESQL_OWNER_BASE64=$(echo -n $3 | base64)
export POSTGRESQL_PASSWORD_BASE64=$(echo -n $4 | base64)

envsubst < "${dir}/pg.yaml" > "${dir}/pg.${POSTGRESQL_DB}.yaml.tmp"
kubectl delete -f "${dir}/pg.${POSTGRESQL_DB}.yaml.tmp" -n ${NAMESPACE} --ignore-not-found
