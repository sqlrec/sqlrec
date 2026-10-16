#!/bin/bash
set -ex

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)"
export DEFAULT_TEST_IP="$(minikube -p minikube ip 2>/dev/null)"
export HADOOP_CONF_DIR="${PROJECT_DIR}/deploy/data/client/hadoop-3.4.0/etc/hadoop"

cd "${PROJECT_DIR}"
exec mvn test -Prun-integration-tests "-Dmaven.test.additionalClasspath=${HADOOP_CONF_DIR}" "$@"
