#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

helm repo add flink-operator-repo https://dlcdn.apache.org/flink/flink-kubernetes-operator-1.12.1/
helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator --set webhook.create=false --namespace "${NAMESPACE}"

envsubst < ${dir}/sql_gateway.yaml > ${dir}/sql_gateway.yaml.tmp
kubectl apply -f "${dir}/sql_gateway.yaml.tmp" -n "${NAMESPACE}"
