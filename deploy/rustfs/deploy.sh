#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

credentials_checksum="$(printf '%s\0%s' "${RUSTFS_ACCESS_KEY}" "${RUSTFS_SECRET_KEY}" | cksum | awk '{print $1}')"

helm upgrade --install rustfs rustfs/rustfs \
  --version "${RUSTFS_VERSION}" \
  --namespace "${NAMESPACE}" \
  --set-string fullnameOverride=rustfs \
  --set replicaCount=1 \
  --set mode.standalone.enabled=true \
  --set mode.distributed.enabled=false \
  --set-string secret.rustfs.access_key="${RUSTFS_ACCESS_KEY}" \
  --set-string secret.rustfs.secret_key="${RUSTFS_SECRET_KEY}" \
  --set-string podAnnotations.sqlrec-credentials-checksum="${credentials_checksum}" \
  --set-string config.rustfs.region="${RUSTFS_REGION}" \
  --set ingress.enabled=false \
  --set service.type=NodePort \
  --set service.endpoint.nodePort="${RUSTFS_PORT}" \
  --set service.console.nodePort="${RUSTFS_CONSOLE_PORT}" \
  --set-string storageclass.name="${RUSTFS_STORAGE_CLASS}" \
  --set-string storageclass.dataStorageSize="${RUSTFS_DATA_STORAGE_SIZE}" \
  --set-string storageclass.logStorageSize="${RUSTFS_LOG_STORAGE_SIZE}" \
  --set resources.requests.memory=2Gi \
  --wait \
  --timeout "${DEPLOY_TIMEOUT}s"

render_config "${dir}/rustfs-buckets.yaml" \
  '${RUSTFS_RC_VERSION} ${RUSTFS_SERVICE_NAME} ${RUSTFS_REGION} ${RUSTFS_JUICEFS_BUCKET} ${RUSTFS_MILVUS_BUCKET}'
kubectl delete job rustfs-create-buckets -n "${NAMESPACE}" --ignore-not-found
kubectl apply -f "${dir}/rustfs-buckets.yaml.tmp" -n "${NAMESPACE}"
wait_for_job rustfs-create-buckets "${NAMESPACE}" "${DEPLOY_TIMEOUT}"
