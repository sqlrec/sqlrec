#!/bin/bash
set -eo pipefail

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)"

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    cat <<'EOF'
Usage: bash bin/start-local.sh [Main arguments...]

Start com.sqlrec.frontend.Main using the local shaded frontend JAR.
Build first: mvn package -pl sqlrec-frontend -am -DskipTests

Dependency addresses follow bin/test.sh: DEFAULT_TEST_IP, then NODE_IP,
then Minikube discovery. Explicit component settings override defaults;
empty settings are preserved. SQLREC_ENV_CONF can name an environment file.
Use JAVA_HOME to select JDK 21+ and JDK_JAVA_OPTIONS for JVM options.
REST/UI and Thrift use their application defaults (30001 and 30000).
Override REST_SERVER_PORT or THRIFT_SERVER_PORT as needed.
Logs default to sqlrec-frontend/target/logs. Press Ctrl+C to stop.
EOF
    exit 0
fi

if [ -n "${SQLREC_ENV_CONF}" ]; then
    source "${SQLREC_ENV_CONF}"
fi

JARS=("${PROJECT_DIR}"/sqlrec-frontend/target/sqlrec-frontend-*.jar)
if [ ! -f "${JARS[0]}" ]; then
    echo "ERROR: No frontend JAR found. Run 'mvn package -pl sqlrec-frontend -am -DskipTests' first." >&2
    exit 1
fi
if [ "${#JARS[@]}" -ne 1 ]; then
    echo "ERROR: Multiple frontend JARs found. Run 'mvn clean package -pl sqlrec-frontend -am -DskipTests' first." >&2
    exit 1
fi

# Match test.sh without sourcing deploy/env.sh, which selects Java 8 for clients.
DEFAULT_TEST_IP="${DEFAULT_TEST_IP:-${NODE_IP:-$(minikube -p minikube ip)}}" || {
    echo "ERROR: set DEFAULT_TEST_IP or start Minikube before starting the local server." >&2
    exit 1
}
if [ -z "${DEFAULT_TEST_IP}" ]; then
    echo "ERROR: set DEFAULT_TEST_IP or start Minikube before starting the local server." >&2
    exit 1
fi
export DEFAULT_TEST_IP
export META_DB_URL="${META_DB_URL-jdbc:postgresql://${DEFAULT_TEST_IP}:${SQLREC_POSTGRESQL_PORT:-30005}/sqlrec?sslmode=disable}"
export META_DB_USER="${META_DB_USER-${SQLREC_POSTGRESQL_USER:-sqlrec}}"
export META_DB_PASSWORD="${META_DB_PASSWORD-${SQLREC_POSTGRESQL_PASSWORD:-abc123456}}"
export HIVE_METASTORE_URI="${HIVE_METASTORE_URI-thrift://${DEFAULT_TEST_IP}:${HMS_PORT:-30008}}"
export FLINK_SQL_GATEWAY_ADDRESS="${FLINK_SQL_GATEWAY_ADDRESS-${DEFAULT_TEST_IP}}"
export TRACE_ENDPOINT="${TRACE_ENDPOINT-http://${DEFAULT_TEST_IP}:${JAEGER_OTLP_GRPC_PORT:-30038}}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${BASE_DIR:-${PROJECT_DIR}/deploy}/data/client/hadoop-${HADOOP_VERSION:-3.4.0}/etc/hadoop}"
export LOG_DIR="${LOG_DIR:-${PROJECT_DIR}/sqlrec-frontend/target/logs}"

# Use the shaded JAR explicitly so original-*.jar is excluded from the classpath.
CLASSPATH="${JARS[0]}:${HADOOP_CONF_DIR}${CLASSPATH:+:${CLASSPATH}}"
if [ -n "${HADOOP_HOME}" ]; then
    export PATH="${PATH}:${HADOOP_HOME}/bin"
    CLASSPATH="${CLASSPATH}:$("${HADOOP_HOME}/bin/hadoop" classpath)"
fi

mkdir -p "${LOG_DIR}"
cd "${PROJECT_DIR}"
exec "${JAVA_HOME:+${JAVA_HOME}/bin/}java" -cp "${CLASSPATH}" com.sqlrec.frontend.Main "$@"
