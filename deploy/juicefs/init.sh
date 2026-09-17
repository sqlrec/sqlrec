#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

if [ ! -f "${CLIENT_DIR}/${JUICEFS_ARCH_NAME}" ]; then
  download_file "${JUICEFS_URL}" "${CLIENT_DIR}/${JUICEFS_ARCH_NAME}"
fi

if [ ! -f "${CLIENT_DIR}/juicefs" ]; then
  tar -xzf "${CLIENT_DIR}/${JUICEFS_ARCH_NAME}" -C "${CLIENT_DIR}"
fi

if command -v juicefs >/dev/null 2>&1; then
  echo "using JuiceFS CLI from $(command -v juicefs)"
else
  echo "ERROR: downloaded JuiceFS CLI is not executable: ${CLIENT_DIR}/juicefs" >&2
  exit 1
fi

if [ ! -f "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}" ]; then
  download_file "${JUICEFS_HADOOP_JAR_URL}" "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}"
fi
