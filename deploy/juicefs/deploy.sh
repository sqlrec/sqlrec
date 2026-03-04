#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

helm upgrade --install juicefs-valkey valkey/valkey \
  --namespace "${NAMESPACE}" \
  --set service.type=NodePort \
  --set service.nodePort=${JUICEFS_REDIS_PORT} \
  --set dataStorage.enabled=true \
  --set dataStorage.requestedSize=128Gi

juicefs format \
    --no-update \
    --storage minio \
    --bucket http://${NODE_IP}:${MINIO_PORT}/bucket1 \
    --access-key ${MINIO_USER} \
    --secret-key ${MINIO_PASSWORD} \
    "redis://${NODE_IP}:${JUICEFS_REDIS_PORT}/0" \
    myjfs