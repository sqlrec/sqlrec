#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

helm upgrade --install mysql-juicefs \
  --namespace "${NAMESPACE}" \
  --set primary.service.type=NodePort \
  --set primary.service.nodePorts.mysql=${JUICEFS_MYSQL_PORT} \
  --set auth.database=juicefs,auth.username=${JUICEFS_MYSQL_USER},auth.password=${JUICEFS_MYSQL_PASSWORD} \
  --wait \
  --timeout 3600s \
  oci://registry-1.docker.io/bitnamicharts/mysql

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
    "mysql://${JUICEFS_MYSQL_USER}:${JUICEFS_MYSQL_PASSWORD}@(${NODE_IP}:${JUICEFS_MYSQL_PORT})/juicefs" \
    myjfs

envsubst < ${dir}/core-site.template > ${CONF_DIR}/core-site.xml
kubectl create configmap core-site --from-file="${CONF_DIR}/core-site.xml" -n "${NAMESPACE}"

cp ${dir}/hdfs-site.xml  ${CONF_DIR}/
kubectl create configmap hdfs-site --from-file="${CONF_DIR}/hdfs-site.xml" -n "${NAMESPACE}"