#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

bash ${dir}/../postgresql/deploy.sh sqlrec ${SQLREC_POSTGRESQL_PORT} ${SQLREC_POSTGRESQL_USER} ${SQLREC_POSTGRESQL_PASSWORD}

export PGPASSWORD=${SQLREC_POSTGRESQL_PASSWORD}
psql -h localhost -p ${SQLREC_POSTGRESQL_PORT} -U ${SQLREC_POSTGRESQL_USER} -d sqlrec -f ${dir}/../sql/master.sql