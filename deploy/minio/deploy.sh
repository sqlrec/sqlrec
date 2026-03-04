#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

helm upgrade --install minio \
 --namespace "${NAMESPACE}" \
 --set resources.requests.memory=2Gi \
 --set replicas=1 \
 --set mode=standalone \
 --set rootUser=${MINIO_USER},rootPassword=${MINIO_PASSWORD} \
 --set service.type=NodePort \
 --set service.nodePort=${MINIO_PORT} \
 --set consoleService.type=NodePort \
 --set consoleService.nodePort=${MINIO_CONSOLE_PORT} \
 --set buckets[0].name=bucket1,buckets[0].policy=none,buckets[0].purge=false \
 --wait \
 --timeout 3600s \
 minio/minio
