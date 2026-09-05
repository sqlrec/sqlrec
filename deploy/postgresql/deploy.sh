#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

export POSTGRESQL_DB=$1
export POSTGRESQL_PORT=$2
export POSTGRESQL_OWNER=$3
export POSTGRESQL_PASSWORD=$4
export POSTGRESQL_OWNER_BASE64=$(echo -n $3 | base64)
export POSTGRESQL_PASSWORD_BASE64=$(echo -n $4 | base64)

envsubst < "${dir}/pg.yaml" > "${dir}/pg.${POSTGRESQL_DB}.yaml.tmp"
kubectl apply -f "${dir}/pg.${POSTGRESQL_DB}.yaml.tmp" -n ${NAMESPACE}
kubectl wait --for=condition=Ready cluster/${POSTGRESQL_DB}-postgresql --timeout=${DEPLOY_TIMEOUT}s -n ${NAMESPACE}
