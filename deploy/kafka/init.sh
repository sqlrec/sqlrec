#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

kubectl create -f "https://strimzi.io/install/latest?namespace=${NAMESPACE}" -n ${NAMESPACE}