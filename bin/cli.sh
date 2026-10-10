#!/bin/bash
set -e

# Resolve the directory where this script lives
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Prefer target/ (development), fall back to bin/ (production deploy)
if compgen -G "${PROJECT_DIR}/sqlrec-frontend/target/sqlrec-frontend-*.jar" >/dev/null; then
    CLASSPATH=""
    for jar in "${PROJECT_DIR}"/sqlrec-frontend/target/*.jar; do
        case "${jar##*/}" in
            original-*) ;;
            *) CLASSPATH="${CLASSPATH:+${CLASSPATH}:}${jar}" ;;
        esac
    done
elif compgen -G "${SCRIPT_DIR}/*.jar" >/dev/null; then
    CLASSPATH="${SCRIPT_DIR}/*"
else
    echo "Error: No JARs found. Run 'mvn package -pl sqlrec-frontend -am -DskipTests' first." >&2
    exit 1
fi

if [ -n "${SQLREC_ENV_CONF}" ]; then
  source "${SQLREC_ENV_CONF}"
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
