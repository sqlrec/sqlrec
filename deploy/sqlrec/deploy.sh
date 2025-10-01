#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

helm upgrade --install postgresql-sqlrec \
  --namespace "${NAMESPACE}" \
  --set primary.service.type=NodePort \
  --set primary.service.nodePorts.postgresql=${SQLREC_POSTGRESQL_PORT} \
  --set auth.database=sqlrec,auth.username=${SQLREC_POSTGRESQL_USER},auth.password=${SQLREC_POSTGRESQL_PASSWORD} \
  --set image.repository=bitnamilegacy/postgresql \
  --wait \
  --timeout 3600s \
  oci://registry-1.docker.io/bitnamicharts/postgresql