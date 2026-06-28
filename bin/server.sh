#!/bin/bash
set -ex

if [ -n "${SQLREC_ENV_CONF}" ]; then
  source "${SQLREC_ENV_CONF}"
fi

export LOG_DIR=${LOG_DIR:-/var/log/sqlrec}
if [ -n "${HADOOP_HOME}" ]; then
  export PATH=$PATH:${HADOOP_HOME}/bin
  export HADOOP_CLASSPATH=`hadoop classpath`
  export CLASSPATH=$CLASSPATH:${HADOOP_CLASSPATH}
  export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}
fi

exec java -cp ./*:${CLASSPATH} com.sqlrec.frontend.Main "$@"