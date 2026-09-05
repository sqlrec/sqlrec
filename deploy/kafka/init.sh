#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

operator_manifest="${dir}/strimzi-cluster-operator.yaml"
trap 'rm -f "${operator_manifest}"' EXIT INT TERM

curl --fail --silent --show-error --location \
  "https://strimzi.io/install/latest?namespace=${NAMESPACE}" \
  --output "${operator_manifest}"
kubectl apply -f "${operator_manifest}" -n "${NAMESPACE}"

rm -f "${operator_manifest}"
trap - EXIT INT TERM
