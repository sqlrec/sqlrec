#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/env.sh

# install docker first：https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository
if command -v docker >/dev/null 2>&1; then
  echo 'skip install docker'
else
  curl -fsSL https://get.docker.com -o get-docker.sh
  sudo sh get-docker.sh
  sudo usermod -aG docker $USER
  newgrp docker
  rm get-docker.sh
fi

# refer to https://minikube.sigs.k8s.io/docs/start/
if command -v minikube >/dev/null 2>&1; then
  echo 'skip install minikube'
else
  if [ ! -f "${CLIENT_DIR}/minikube-linux-amd64" ];then
    wget -P "${CLIENT_DIR}" "${MINIKUBE_URL}"
  fi
  sudo install "${CLIENT_DIR}/minikube-linux-amd64" /usr/local/bin/minikube
  alias kubectl="minikube kubectl --"
  echo 'alias kubectl="minikube kubectl --"' >> ~/.bash_profile
fi

bash ${dir}/docker-registry/deploy.sh

minikube start \
 --driver=docker \
  --cpus='no-limit' \
  --memory='no-limit' \
  --mount \
  --mount-string="${DATA_DIR}:${DATA_DIR}" \
  --ports="${SQL_GATEWAY_PORT}:${SQL_GATEWAY_PORT}" \
  --ports="${HMS_MYSQL_PORT}:${HMS_MYSQL_PORT}" \
  --ports="${HMS_PORT}:${HMS_PORT}" \
  --ports="${JUICEFS_MYSQL_PORT}:${JUICEFS_MYSQL_PORT}" \
  --ports="${MINIO_PORT}:${MINIO_PORT}" \
  --ports="${MINIO_CONSOLE_PORT}:${MINIO_CONSOLE_PORT}" \
  --registry-mirror=http://${IMAGE_REGISTRY_URL} \
  --insecure-registry=${IMAGE_REGISTRY_URL}

minikube addons enable storage-provisioner-rancher

envsubst < ${dir}/pv.yaml > ${dir}/pv.yaml.tmp
kubectl apply -f ${dir}/pv.yaml.tmp -n ${NAMESPACE}

# refer to https://helm.sh/docs/intro/install/
if command -v helm >/dev/null 2>&1; then
  echo 'skip install helm'
else
  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi
