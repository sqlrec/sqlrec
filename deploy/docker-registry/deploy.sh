#!/bin/bash
set -ex
dir=$(dirname $(realpath $0))

if sudo lsof -i :${IMAGE_REGISTRY_PORT} > /dev/null; then
  echo "registry port ${IMAGE_REGISTRY_PORT} is used, skip deploy"
  exit 0
fi

docker run -d -p ${IMAGE_REGISTRY_PORT}:5000 --restart=always --name registry \
             -v ${dir}/config.yml:/etc/docker/registry/config.yml \
             -v ${LOCAL_REGISTRY_DIR}:/var/lib/registry \
             registry:2
