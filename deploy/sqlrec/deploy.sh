#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

if ! kubectl get serviceaccount sqlrec -n "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl create serviceaccount sqlrec -n "${NAMESPACE}"
  kubectl create clusterrolebinding sqlrec-role --clusterrole=edit --serviceaccount="${NAMESPACE}":sqlrec --namespace="${NAMESPACE}"
fi

bash "${dir}/../postgresql/deploy.sh" sqlrec ${SQLREC_POSTGRESQL_PORT} ${SQLREC_POSTGRESQL_USER} ${SQLREC_POSTGRESQL_PASSWORD}

export PGPASSWORD=${SQLREC_POSTGRESQL_PASSWORD}
PSQL_MAX_ATTEMPTS="${PSQL_MAX_ATTEMPTS:-30}"
PSQL_RETRY_INTERVAL="${PSQL_RETRY_INTERVAL:-5}"
for ((attempt = 1; attempt <= PSQL_MAX_ATTEMPTS; attempt++)); do
  if psql -h "${NODE_IP}" -p "${SQLREC_POSTGRESQL_PORT}" -U "${SQLREC_POSTGRESQL_USER}" -d sqlrec -f "${dir}/../sql/master.sql"; then
    break
  fi
  if [ "${attempt}" -eq "${PSQL_MAX_ATTEMPTS}" ]; then
    echo "ERROR: failed to initialize PostgreSQL after ${PSQL_MAX_ATTEMPTS} attempts." >&2
    exit 1
  fi
  echo "PostgreSQL is not ready; retrying in ${PSQL_RETRY_INTERVAL}s (${attempt}/${PSQL_MAX_ATTEMPTS})..." >&2
  sleep "${PSQL_RETRY_INTERVAL}"
done

DEFAULT_JAVA_TOOL_OPTIONS="-XX:+UseCompactObjectHeaders -XX:+UseStringDeduplication"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-${DEFAULT_JAVA_TOOL_OPTIONS}}"
if [ "${DEBUG_MODE}" = "true" ]; then
    export JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${SQLREC_DEBUG_PORT}"
fi

export DEBUG_TRACE="${DEBUG_TRACE:-false}"
export TRACE_ENDPOINT="${TRACE_ENDPOINT:-http://${NODE_IP}:${JAEGER_OTLP_GRPC_PORT}}"
export TRACE_SERVICE_NAME="${TRACE_SERVICE_NAME:-sqlrec}"

render_config "${dir}/sqlrec.yaml"
kubectl apply -f "${dir}/sqlrec.yaml.tmp" -n "${NAMESPACE}"
