#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/config.yaml > ${dir}/config.sub.yaml

helm upgrade --install jupyterhub jupyterhub/jupyterhub \
  --namespace ${NAMESPACE} \
  --version=4.3.1 \
  --values ${dir}/config.sub.yaml