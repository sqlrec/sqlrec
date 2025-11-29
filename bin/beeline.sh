#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
set -ex
dir=$(dirname $(realpath $0))

export BASE_DIR=$(dirname ${dir})/deploy
source ${dir}/../deploy/env.sh

beeline -u "jdbc:hive2://127.0.0.1:${SQLREC_THRIFT_PORT}/default;auth=noSasl"