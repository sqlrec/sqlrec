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
