#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

helm upgrade --install juicefs-valkey valkey/valkey \
  --namespace "${NAMESPACE}" \
  --set image.tag="${VALKEY_VERSION}" \
  --set service.type=NodePort \
  --set service.nodePort="${JUICEFS_REDIS_PORT}" \
  --set dataStorage.enabled=true \
  --set dataStorage.requestedSize=128Gi \
  --set valkeyConfig="appendonly yes" \
  --wait \
  --timeout "${DEPLOY_TIMEOUT}s"

AWS_REGION="${RUSTFS_REGION}" AWS_DEFAULT_REGION="${RUSTFS_REGION}" juicefs format \
    --no-update \
    --storage s3 \
    --bucket "http://${NODE_IP}:${RUSTFS_PORT}/${RUSTFS_JUICEFS_BUCKET}" \
    --access-key "${RUSTFS_ACCESS_KEY}" \
    --secret-key "${RUSTFS_SECRET_KEY}" \
    "redis://${NODE_IP}:${JUICEFS_REDIS_PORT}/0" \
    myjfs
