#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

namespace=$1

# refer to https://github.com/bitnami/charts/tree/main/bitnami/redis
helm install redis \
  --namespace "$namespace" \
  --set architecture=standalone \
  --set master.service.type=NodePort \
  --set auth.enabled=false \
  oci://registry-1.docker.io/bitnamicharts/redis
