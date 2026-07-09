#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

helm uninstall opensearch-dashboards -n ${NAMESPACE} --ignore-not-found
helm uninstall opensearch -n ${NAMESPACE} --ignore-not-found
