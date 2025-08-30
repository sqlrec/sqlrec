#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

helm upgrade --install mysql-sqlrec \
  --namespace "${NAMESPACE}" \
  --set primary.service.type=NodePort \
  --set primary.service.nodePorts.mysql=${SQLREC_MYSQL_PORT} \
  --set auth.database=sqlrec,auth.username=${SQLREC_MYSQL_USER},auth.password=${SQLREC_MYSQL_PASSWORD} \
  --wait \
  --timeout 3600s \
  oci://registry-1.docker.io/bitnamicharts/mysql