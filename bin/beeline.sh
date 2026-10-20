#!/bin/bash
set +x
set -eo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"

source "${dir}/../deploy/env.sh"

url="jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default"
if [ "${AUTH_ENABLED}" = true ]; then
  exec beeline -u "${url}" -n "${AUTH_USERNAME}" -p "${AUTH_PASSWORD}" "$@"
fi
exec beeline -u "${url};auth=noSasl" "$@"
