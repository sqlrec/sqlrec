#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

# refer to https://github.com/bitnami/charts/tree/main/bitnami/kafka
helm upgrade --install kafka \
  --namespace "${NAMESPACE}" \
  --set controller.replicaCount=1 \
  --set controller.protocol=PLAINTEXT \
  --set controller.resourcesPreset=medium \
  --set externalAccess.enabled=true \
  --set externalAccess.controller.service.type=NodePort \
  --set externalAccess.controller.service.nodePorts[0]=${KAFKA_PORT1} \
  oci://registry-1.docker.io/bitnamicharts/kafka

