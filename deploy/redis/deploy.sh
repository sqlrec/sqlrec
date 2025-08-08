#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

# refer to https://github.com/bitnami/charts/tree/main/bitnami/redis
helm upgrade --install redis \
  --namespace "${NAMESPACE}" \
  --set architecture=standalone \
  --set master.service.type=NodePort \
  --set master.service.nodePorts.redis=${REDIS_PORT} \
  --set auth.enabled=false \
  --version 18.17.1 \
  oci://registry-1.docker.io/bitnamicharts/redis
