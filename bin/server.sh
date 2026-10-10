#!/bin/bash
set -ex

if [ -n "${SQLREC_ENV_CONF}" ]; then
  source "${SQLREC_ENV_CONF}"
fi

export LOG_DIR=${LOG_DIR:-/var/log/sqlrec}
if [ -n "${HADOOP_HOME}" ]; then
  export PATH="${PATH}:${HADOOP_HOME}/bin"
  export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}"
  export HADOOP_CLASSPATH="$(hadoop classpath)"
  export CLASSPATH="${CLASSPATH}:${HADOOP_CLASSPATH}"
fi

# Configuration must be on the JVM classpath even without a Hadoop installation.
if [ -n "${HADOOP_CONF_DIR}" ]; then
  export CLASSPATH="${HADOOP_CONF_DIR}:${CLASSPATH}"
fi
export CLASSPATH="./*:${CLASSPATH}"

exec java -cp "${CLASSPATH}" com.sqlrec.frontend.Main "$@"
