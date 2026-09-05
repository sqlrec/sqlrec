#!/bin/bash
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"

export BASE_DIR=$(dirname ${dir})/deploy
source ${dir}/../deploy/env.sh

beeline -u "jdbc:hive2://${NODE_IP}:${SQLREC_THRIFT_PORT}/default;auth=noSasl" "$@"