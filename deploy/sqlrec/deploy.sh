#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

bash ${dir}/../postgresql/deploy.sh sqlrec ${SQLREC_POSTGRESQL_PORT} ${SQLREC_POSTGRESQL_USER} ${SQLREC_POSTGRESQL_PASSWORD}