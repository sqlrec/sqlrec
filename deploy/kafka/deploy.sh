#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

namespace="${1:-sqlrec}"

# refer to https://github.com/bitnami/charts/tree/main/bitnami/kafka
helm install kafka \
  --namespace "$namespace" \
  --set controller.replicaCount=1 \
  --set controller.protocol=PLAINTEXT \
  --set controller.resourcesPreset=medium \
  oci://registry-1.docker.io/bitnamicharts/kafka

