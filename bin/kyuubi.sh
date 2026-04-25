#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

export BASE_DIR=$(dirname ${dir})/deploy
source ${dir}/../deploy/env.sh

beeline -u "jdbc:hive2://${NODE_IP}:${KYUUBI_PORT}/default?kyuubi.session.engine.idle.timeout=PT1M" "$@"
