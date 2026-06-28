#!/bin/bash
set -e

# Resolve the directory where this script lives
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Prefer target/ (development), fall back to bin/ (production deploy)
if ls "${PROJECT_DIR}"/sqlrec-frontend/target/sqlrec-frontend-*.jar &>/dev/null; then
    CLASSPATH="${PROJECT_DIR}/sqlrec-frontend/target/sqlrec-frontend-*.jar"
    for jar in "${PROJECT_DIR}"/sqlrec-frontend/target/*.jar; do
        case "$(basename "$jar")" in
            original-*) ;;
            *) CLASSPATH="${CLASSPATH}:${jar}" ;;
        esac
    done
elif ls "${SCRIPT_DIR}"/*.jar &>/dev/null; then
    CLASSPATH="${SCRIPT_DIR}/*"
else
    echo "Error: No JARs found. Run 'mvn package -pl sqlrec-frontend -am -DskipTests' first." >&2
    exit 1
fi

# Add Hadoop classpath if available
if [ -n "${HADOOP_HOME}" ]; then
    export PATH="${PATH}:${HADOOP_HOME}/bin"
    HADOOP_CLASSPATH="$(hadoop classpath 2>/dev/null || true)"
    if [ -n "${HADOOP_CLASSPATH}" ]; then
        CLASSPATH="${CLASSPATH}:${HADOOP_CLASSPATH}"
    fi
fi

# Suppress log noise: only show WARN and above on the console
LOG_LEVEL="${LOG_LEVEL:-warn}"

exec java \
    -Dlog.level="${LOG_LEVEL}" \
    -cp "${CLASSPATH}" \
    com.sqlrec.frontend.Cli "$@"
