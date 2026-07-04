#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

helm upgrade --install valkey valkey/valkey \
  --namespace "${NAMESPACE}" \
  --set image.tag=${VALKEY_VERSION} \
  --set service.type=NodePort \
  --set service.nodePort=${REDIS_PORT} \
  --set dataStorage.enabled=true \
  --set dataStorage.requestedSize=128Gi
