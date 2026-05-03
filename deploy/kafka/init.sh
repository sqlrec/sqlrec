#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

curl -sL "https://strimzi.io/install/latest?namespace=${NAMESPACE}" -o ${dir}/strimzi-cluster-operator.yaml
kubectl apply -f ${dir}/strimzi-cluster-operator.yaml -n ${NAMESPACE}