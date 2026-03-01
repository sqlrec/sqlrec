#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

if ! kubectl get serviceaccount sqlrec -n "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl create serviceaccount sqlrec -n "${NAMESPACE}"
  kubectl create clusterrolebinding sqlrec-role --clusterrole=edit --serviceaccount="${NAMESPACE}":sqlrec --namespace="${NAMESPACE}"
fi

bash ${dir}/../postgresql/deploy.sh sqlrec ${SQLREC_POSTGRESQL_PORT} ${SQLREC_POSTGRESQL_USER} ${SQLREC_POSTGRESQL_PASSWORD}

export PGPASSWORD=${SQLREC_POSTGRESQL_PASSWORD}
psql -h localhost -p ${SQLREC_POSTGRESQL_PORT} -U ${SQLREC_POSTGRESQL_USER} -d sqlrec -f ${dir}/../sql/master.sql

envsubst < ${dir}/sqlrec.yaml > ${dir}/sqlrec.yaml.tmp
kubectl apply -f ${dir}/sqlrec.yaml.tmp -n "${NAMESPACE}"
