#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

helm upgrade --install minio \
 --namespace "${NAMESPACE}" \
 --set resources.requests.memory=2Gi \
 --set replicas=1 \
 --set mode=standalone \
 --set-string rootUser="${MINIO_USER}" \
 --set-string rootPassword="${MINIO_PASSWORD}" \
 --set service.type=NodePort \
 --set service.nodePort=${MINIO_PORT} \
 --set consoleService.type=NodePort \
 --set consoleService.nodePort=${MINIO_CONSOLE_PORT} \
 --set buckets[0].name=bucket1,buckets[0].policy=none,buckets[0].purge=false \
 --wait \
 --timeout "${DEPLOY_TIMEOUT}s" \
 minio/minio
