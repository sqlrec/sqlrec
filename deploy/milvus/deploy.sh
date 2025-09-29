#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

helm upgrade --install milvus \
  --namespace "${NAMESPACE}-milvus" \
  --set image.all.tag=v2.6.2 \
  --set cluster.enabled=false \
  --set pulsarv3.enabled=false \
  --set standalone.messageQueue=woodpecker \
  --set woodpecker.enabled=true \
  --set streaming.enabled=true \
  --set service.type=NodePort \
  --set service.nodePort=${MILVUS_PORT} \
  --set minio.mode=standalone \
  zilliztech/milvus
