#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
set -ex
dir=$(dirname $(realpath $0))

cd ${dir}/..

docker build -t sqlrec/sqlrec -f ./docker/Dockerfile .
if command -v minikube >/dev/null 2>&1; then
  minikube image load sqlrec/sqlrec
fi

#docker build -t sqlrec/tzrec -f ./docker/sqlrec-model-tzrec.Dockerfile .
#if command -v minikube >/dev/null 2>&1; then
#  minikube image load sqlrec/tzrec
#fi
