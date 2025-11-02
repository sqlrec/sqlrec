#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

bash ${dir}/../postgresql/deploy.sh juicefs ${JUICEFS_POSTGRESQL_PORT} ${JUICEFS_POSTGRESQL_USER} ${JUICEFS_POSTGRESQL_PASSWORD}

if command -v juicefs >/dev/null 2>&1; then
  echo 'juicefs has installed'
else
  sudo install ${CLIENT_DIR}/juicefs /usr/local/bin
fi

juicefs format \
    --storage minio \
    --bucket http://${NODE_IP}:${MINIO_PORT}/bucket1 \
    --access-key ${MINIO_USER} \
    --secret-key ${MINIO_PASSWORD} \
    "postgres://${JUICEFS_POSTGRESQL_USER}:${JUICEFS_POSTGRESQL_PASSWORD}@${NODE_IP}:${JUICEFS_POSTGRESQL_PORT}/juicefs?sslmode=disable" \
    myjfs

envsubst < ${dir}/core-site.template > ${CONF_DIR}/core-site.xml
if kubectl get configmap core-site -n "${NAMESPACE}"; then
  kubectl delete configmap core-site -n "${NAMESPACE}"
fi
kubectl create configmap core-site --from-file="${CONF_DIR}/core-site.xml" -n "${NAMESPACE}"

cp ${dir}/hdfs-site.xml  ${CONF_DIR}/
if kubectl get configmap hdfs-site -n "${NAMESPACE}"; then
  kubectl delete configmap hdfs-site -n "${NAMESPACE}"
fi
kubectl create configmap hdfs-site --from-file="${CONF_DIR}/hdfs-site.xml" -n "${NAMESPACE}"