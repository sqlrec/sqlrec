#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

operator_manifest="${dir}/strimzi-cluster-operator.yaml"
trap 'rm -f "${operator_manifest}"' EXIT INT TERM

curl --fail --silent --show-error --location \
  "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml" \
  | sed "s/namespace: myproject/namespace: ${NAMESPACE}/g" > "${operator_manifest}"
kubectl apply -f "${operator_manifest}" -n "${NAMESPACE}"

# Skip DNS auto-detection, which can mistake an IPv6 lookup result for a domain.
kubectl set env deployment/strimzi-cluster-operator -n "${NAMESPACE}" \
  "KUBERNETES_SERVICE_DNS_DOMAIN=${KUBERNETES_SERVICE_DNS_DOMAIN}"
kubectl rollout status deployment/strimzi-cluster-operator -n "${NAMESPACE}" \
  --timeout="${DEPLOY_TIMEOUT}s"

rm -f "${operator_manifest}"
trap - EXIT INT TERM
