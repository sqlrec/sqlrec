#!/bin/bash
set -ex
dir=$(dirname $(realpath $0))

if ! docker inspect registry &> /dev/null; then
  docker run -d -p ${IMAGE_REGISTRY_PORT}:5000 --restart=always --name registry \
               -v ${dir}/config.yml:/etc/docker/registry/config.yml \
               -v ${LOCAL_REGISTRY_DIR}:/var/lib/registry \
               registry:2
else
  echo 'skip install docker registry'
fi
