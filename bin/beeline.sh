#!/bin/bash
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"

export BASE_DIR=$(dirname ${dir})/deploy
source "${dir}/../deploy/env.sh"

url="jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default"
case "${AUTH_ENABLED:-false}" in
  [Tt][Rr][Uu][Ee]) ;; # Hive JDBC uses SASL PLAIN for username/password authentication.
  *) url="${url};auth=noSasl" ;;
esac
beeline -u "${url}" "$@"
