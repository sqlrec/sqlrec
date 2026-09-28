#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

PSQL_MAX_ATTEMPTS="${PSQL_MAX_ATTEMPTS:-30}"
PSQL_RETRY_INTERVAL="${PSQL_RETRY_INTERVAL:-5}"
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-10}"
if ! [[ "${PSQL_MAX_ATTEMPTS}" =~ ^[1-9][0-9]*$ ]] || ! [ "${PSQL_MAX_ATTEMPTS}" -gt 0 ]; then
  echo "ERROR: PSQL_MAX_ATTEMPTS must be a positive integer." >&2
  exit 1
fi
if ! [[ "${PSQL_RETRY_INTERVAL}" =~ ^(0|[1-9][0-9]*)$ ]] || ! [ "${PSQL_RETRY_INTERVAL}" -ge 0 ]; then
  echo "ERROR: PSQL_RETRY_INTERVAL must be a non-negative integer." >&2
  exit 1
fi
if ! [[ "${PGCONNECT_TIMEOUT}" =~ ^[1-9][0-9]*$ ]] || ! [ "${PGCONNECT_TIMEOUT}" -gt 0 ]; then
  echo "ERROR: PGCONNECT_TIMEOUT must be a positive integer." >&2
  exit 1
fi
require_commands psql

kubectl create serviceaccount sqlrec -n "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create clusterrolebinding sqlrec-role \
  --clusterrole=edit \
  --serviceaccount="${NAMESPACE}:sqlrec" \
  --dry-run=client -o yaml | kubectl apply -f -

bash "${dir}/../postgresql/deploy.sh" sqlrec "${SQLREC_POSTGRESQL_PORT}" "${SQLREC_POSTGRESQL_USER}" "${SQLREC_POSTGRESQL_PASSWORD}"

export PGPASSWORD="${SQLREC_POSTGRESQL_PASSWORD}"
psql_args=(-X -w -v ON_ERROR_STOP=1 -h "${NODE_IP}" -p "${SQLREC_POSTGRESQL_PORT}" -U "${SQLREC_POSTGRESQL_USER}" -d sqlrec)
# Retry connection readiness only; SQL errors must stop deployment immediately.
for ((attempt = 1; attempt <= PSQL_MAX_ATTEMPTS; attempt++)); do
  if psql "${psql_args[@]}" -c 'SELECT 1' >/dev/null; then
    break
  fi
  if [ "${attempt}" -eq "${PSQL_MAX_ATTEMPTS}" ]; then
    echo "ERROR: PostgreSQL is not ready after ${PSQL_MAX_ATTEMPTS} attempts." >&2
    exit 1
  fi
  echo "PostgreSQL is not ready; retrying in ${PSQL_RETRY_INTERVAL}s (${attempt}/${PSQL_MAX_ATTEMPTS})..." >&2
  sleep "${PSQL_RETRY_INTERVAL}"
done

if ! psql "${psql_args[@]}" --single-transaction -f "${dir}/../sql/master.sql"; then
  echo "ERROR: failed to initialize SQLRec schema; stopping deployment." >&2
  exit 1
fi

DEFAULT_JAVA_TOOL_OPTIONS="-XX:+UseCompactObjectHeaders -XX:+UseStringDeduplication"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-${DEFAULT_JAVA_TOOL_OPTIONS}}"
if [ "${DEBUG_MODE:-false}" = "true" ]; then
    export JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${SQLREC_DEBUG_PORT}"
fi

export DEBUG_TRACE="${DEBUG_TRACE:-false}"
export TRACE_ENDPOINT="${TRACE_ENDPOINT:-http://${NODE_IP}:${JAEGER_OTLP_GRPC_PORT}}"
export TRACE_SERVICE_NAME="${TRACE_SERVICE_NAME:-sqlrec}"

render_config "${dir}/sqlrec.yaml"
kubectl apply -f "${dir}/sqlrec.yaml.tmp" -n "${NAMESPACE}"
