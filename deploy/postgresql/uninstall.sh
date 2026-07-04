#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

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

envsubst < ${dir}/pg.yaml > ${dir}/pg.yaml.${POSTGRESQL_DB}
kubectl delete -f ${dir}/pg.yaml.${POSTGRESQL_DB} -n ${NAMESPACE} --ignore-not-found
