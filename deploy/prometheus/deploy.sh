#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

helm upgrade --install prometheus prometheus-community/kube-prometheus-stack -n ${NAMESPACE} \
  --set grafana.service.type=NodePort \
  --set grafana.service.nodePort=${GRAFANA_PORT} \
  --set prometheus.service.type=NodePort \
  --set prometheus.service.nodePort=${PROMETHEUS_PORT}
