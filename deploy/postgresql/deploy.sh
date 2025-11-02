#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

export POSTGRESQL_DB=$1
export POSTGRESQL_PORT=$2
export POSTGRESQL_OWNER=$3
export POSTGRESQL_PASSWORD=$4
export POSTGRESQL_OWNER_BASE64=$(echo -n $3 | base64)
export POSTGRESQL_PASSWORD_BASE64=$(echo -n $4 | base64)

envsubst < ${dir}/pg.yaml > ${dir}/pg.yaml.${POSTGRESQL_DB}
kubectl apply -f ${dir}/pg.yaml.${POSTGRESQL_DB} -n ${NAMESPACE}
kubectl wait --for=condition=Ready cluster/${POSTGRESQL_DB}-postgresql --timeout=1800s -n ${NAMESPACE}
