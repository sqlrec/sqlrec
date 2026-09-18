#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

helm upgrade --install milvus \
  --namespace "${NAMESPACE}-milvus" \
  --set image.all.tag=${MILVUS_VERSION} \
  --set cluster.enabled=false \
  --set pulsarv3.enabled=false \
  --set standalone.messageQueue=woodpecker \
  --set woodpecker.enabled=true \
  --set streaming.enabled=true \
  --set service.type=NodePort \
  --set service.nodePort=${MILVUS_PORT} \
  --set minio.enabled=false \
  --set externalS3.enabled=true \
  --set-string externalS3.host="${RUSTFS_SERVICE_NAME}.${NAMESPACE}.svc.cluster.local" \
  --set externalS3.port=9000 \
  --set-string externalS3.accessKey="${RUSTFS_ACCESS_KEY}" \
  --set-string externalS3.secretKey="${RUSTFS_SECRET_KEY}" \
  --set-string externalS3.bucketName="${RUSTFS_MILVUS_BUCKET}" \
  --set-string externalS3.rootPath="${RUSTFS_MILVUS_ROOT_PATH}" \
  --set externalS3.useSSL=false \
  --set-string externalS3.cloudProvider=aws \
  --set-string externalS3.region="${RUSTFS_REGION}" \
  --set externalS3.useVirtualHost=false \
  zilliztech/milvus
