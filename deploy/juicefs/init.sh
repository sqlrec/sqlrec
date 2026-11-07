#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

if [ ! -f "${CLIENT_DIR}/${JUICEFS_ARCH_NAME}" ]; then
  download_file "${JUICEFS_URL}" "${CLIENT_DIR}/${JUICEFS_ARCH_NAME}"
fi

# The CLI has an unversioned filename; replace it from the selected archive
# on every init so changing JFS_LATEST_TAG also upgrades an existing client.
tar -xzf "${CLIENT_DIR}/${JUICEFS_ARCH_NAME}" -C "${CLIENT_DIR}"

if [ -x "${CLIENT_DIR}/juicefs" ]; then
  "${CLIENT_DIR}/juicefs" version
else
  echo "ERROR: downloaded JuiceFS CLI is not executable: ${CLIENT_DIR}/juicefs" >&2
  exit 1
fi

if [ ! -f "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}" ]; then
  download_file "${JUICEFS_HADOOP_JAR_URL}" "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}"
fi
