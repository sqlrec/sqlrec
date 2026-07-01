#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/jaeger.yaml > ${dir}/jaeger.yaml.rendered
kubectl apply -f ${dir}/jaeger.yaml.rendered -n ${NAMESPACE}
kubectl wait --for=condition=Ready pod -l app=jaeger --timeout=600s -n ${NAMESPACE}
