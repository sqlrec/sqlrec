#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

kubectl delete -f ${dir}/kyuubi.yaml -n ${NAMESPACE} --ignore-not-found
