#!/bin/bash
set -e

# Resolve the directory where this script lives
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Prefer target/ (development), fall back to bin/ (production deploy)
if compgen -G "${PROJECT_DIR}/sqlrec-frontend/target/sqlrec-frontend-*.jar" >/dev/null; then
    LOCAL_MODE=true
    CLASSPATH=""
    for jar in "${PROJECT_DIR}"/sqlrec-frontend/target/*.jar; do
        case "${jar##*/}" in
            original-*) ;;
            *) CLASSPATH="${CLASSPATH:+${CLASSPATH}:}${jar}" ;;
        esac
    done
elif compgen -G "${SCRIPT_DIR}/*.jar" >/dev/null; then
    LOCAL_MODE=false
    CLASSPATH="${SCRIPT_DIR}/*"
else
    echo "Error: No JARs found. Run 'mvn package -pl sqlrec-frontend -am -DskipTests' first." >&2
    exit 1
fi

if [ -n "${SQLREC_ENV_CONF}" ]; then
  source "${SQLREC_ENV_CONF}"
fi

if [ "${LOCAL_MODE}" = true ]; then
    NEEDS_METADATA=true
    for arg in "$@"; do
        case "${arg}" in
            -h|--help|-V|--version) NEEDS_METADATA=false ;;
        esac
    done
    if [ "${NEEDS_METADATA}" = true ] && [ -z "${SQL_SCHEMA_DIR}" ]; then
        # Follow test.sh defaults while preserving explicitly configured endpoints,
        # including empty values. A configured HMS does not require Minikube.
        DEFAULT_TEST_IP="${DEFAULT_TEST_IP:-${NODE_IP}}"
        if [ -z "${DEFAULT_TEST_IP}" ] && [ -z "${HIVE_METASTORE_URI+x}" ]; then
            DEFAULT_TEST_IP="$(minikube -p minikube ip)"
        fi
        if [ -n "${DEFAULT_TEST_IP}" ]; then
            export DEFAULT_TEST_IP
            export META_DB_URL="${META_DB_URL-jdbc:postgresql://${DEFAULT_TEST_IP}:${SQLREC_POSTGRESQL_PORT:-30005}/sqlrec?sslmode=disable}"
            export META_DB_USER="${META_DB_USER-${SQLREC_POSTGRESQL_USER:-sqlrec}}"
            export META_DB_PASSWORD="${META_DB_PASSWORD-${SQLREC_POSTGRESQL_PASSWORD:-abc123456}}"
            export HIVE_METASTORE_URI="${HIVE_METASTORE_URI-thrift://${DEFAULT_TEST_IP}:${HMS_PORT:-30008}}"
            export FLINK_SQL_GATEWAY_ADDRESS="${FLINK_SQL_GATEWAY_ADDRESS-${DEFAULT_TEST_IP}}"
            export TRACE_ENDPOINT="${TRACE_ENDPOINT-http://${DEFAULT_TEST_IP}:${JAEGER_OTLP_GRPC_PORT:-30038}}"
        fi
    fi
    if [ -z "${HADOOP_HOME}" ]; then
        export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${BASE_DIR:-${PROJECT_DIR}/deploy}/data/client/hadoop-${HADOOP_VERSION:-3.4.3}/etc/hadoop}"
    fi
    export LOG_DIR="${LOG_DIR:-${PROJECT_DIR}/sqlrec-frontend/target/logs}"
fi

# Add Hadoop classpath if available
if [ -n "${HADOOP_HOME}" ]; then
    export PATH="${PATH}:${HADOOP_HOME}/bin"
    export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}"
    HADOOP_CLASSPATH="$(hadoop classpath 2>/dev/null || true)"
fi

if [ -n "${HADOOP_CONF_DIR}" ]; then
    CLASSPATH="${CLASSPATH}:${HADOOP_CONF_DIR}"
fi

if [ -n "${HADOOP_HOME}" ] && [ -n "${HADOOP_CLASSPATH}" ]; then
    CLASSPATH="${CLASSPATH}:${HADOOP_CLASSPATH}"
fi

export LOG_DIR=${LOG_DIR:-/var/log/sqlrec}
export LOG_LEVEL=${LOG_LEVEL:-warn}

exec java -cp "${CLASSPATH}" com.sqlrec.frontend.Cli "$@"
