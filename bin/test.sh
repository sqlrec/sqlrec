#!/bin/bash
set -eo pipefail

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)"
DEFAULT_TEST_IP="${DEFAULT_TEST_IP:-$(minikube -p minikube ip)}"
# Export for SqlRecConfigs.DEFAULT_TEST_IP and demo SQL environment substitution.
export DEFAULT_TEST_IP
export META_DB_URL="${META_DB_URL-jdbc:postgresql://${DEFAULT_TEST_IP}:30005/sqlrec?sslmode=disable}"
export HIVE_METASTORE_URI="${HIVE_METASTORE_URI-thrift://${DEFAULT_TEST_IP}:30008}"
export FLINK_SQL_GATEWAY_ADDRESS="${FLINK_SQL_GATEWAY_ADDRESS-${DEFAULT_TEST_IP}}"
export TRACE_ENDPOINT="${TRACE_ENDPOINT-http://${DEFAULT_TEST_IP}:30038}"
export HADOOP_CONF_DIR="${PROJECT_DIR}/deploy/data/client/hadoop-3.4.0/etc/hadoop"

cd "${PROJECT_DIR}"
exec mvn test -Prun-integration-tests "-Dmaven.test.additionalClasspath=${HADOOP_CONF_DIR}" "$@"
