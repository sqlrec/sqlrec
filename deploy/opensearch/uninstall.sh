#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

helm uninstall opensearch-dashboards -n ${NAMESPACE} --ignore-not-found
helm uninstall opensearch -n ${NAMESPACE} --ignore-not-found
