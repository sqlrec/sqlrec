#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

source ${dir}/../deploy/env.sh
cd ${dir}/..

if command -v minikube >/dev/null 2>&1; then
  eval $(minikube -p minikube docker-env)
fi

docker image prune -f
docker build -t sqlrec/tzrec:${SQLREC_VERSION}-cpu -f ./docker/sqlrec-model-tzrec.Dockerfile .
docker build -t sqlrec/gbdt:${SQLREC_VERSION}-cpu -f ./docker/sqlrec-model-gbdt.Dockerfile .
docker build -t sqlrec/transformers:${SQLREC_VERSION} -f ./docker/sqlrec-model-transformers.Dockerfile .