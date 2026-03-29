#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

if ! kubectl get serviceaccount sqlrec -n "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl create serviceaccount sqlrec -n "${NAMESPACE}"
  kubectl create clusterrolebinding sqlrec-role --clusterrole=edit --serviceaccount="${NAMESPACE}":sqlrec --namespace="${NAMESPACE}"
fi

bash ${dir}/../postgresql/deploy.sh sqlrec ${SQLREC_POSTGRESQL_PORT} ${SQLREC_POSTGRESQL_USER} ${SQLREC_POSTGRESQL_PASSWORD}

export PGPASSWORD=${SQLREC_POSTGRESQL_PASSWORD}
psql -h ${NODE_IP} -p ${SQLREC_POSTGRESQL_PORT} -U ${SQLREC_POSTGRESQL_USER} -d sqlrec -f ${dir}/../sql/master.sql

DEFAULT_JAVA_TOOL_OPTIONS="-XX:+UseCompactObjectHeaders -XX:+UseStringDeduplication"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-${DEFAULT_JAVA_TOOL_OPTIONS}}"
if [ "${DEBUG_MODE}" = "true" ]; then
    export JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${SQLREC_DEBUG_PORT}"
fi

envsubst < ${dir}/sqlrec.yaml > ${dir}/sqlrec.yaml.tmp
kubectl apply -f ${dir}/sqlrec.yaml.tmp -n "${NAMESPACE}"
