#!/bin/bash
set -eo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

if [ ! -f "${CLIENT_DIR}/${KYUUBI_CLIENT_ARCH_NAME}" ]; then
  download_file "${KYUUBI_CLIENT_URL}" "${CLIENT_DIR}/${KYUUBI_CLIENT_ARCH_NAME}"
fi

if [ ! -d "${CLIENT_DIR}/${KYUUBI_CLIENT_DIR_NAME}" ]; then
  tar -xzf "${CLIENT_DIR}/${KYUUBI_CLIENT_ARCH_NAME}" -C "${CLIENT_DIR}"
fi

# Older manifests mounted the JuiceFS fat JAR into Kyuubi's own jars directory.
# Besides conflicting with Kyuubi's Jersey/ASM dependencies, a subPath mount can
# leave an empty mount-point file behind on the shared client volume.
legacy_juicefs_jar="${KYUUBI_HOME}/jars/juicefs-hadoop.jar"
if [ -e "${legacy_juicefs_jar}" ]; then
  rm -f "${legacy_juicefs_jar}"
fi
