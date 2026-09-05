#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

envsubst < ${dir}/config.yaml > ${dir}/config.sub.yaml

helm upgrade --install jupyterhub jupyterhub/jupyterhub \
  --namespace ${NAMESPACE} \
  --version=${JUPYTERHUB_VERSION} \
  --values ${dir}/config.sub.yaml