#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

kubectl delete job rustfs-create-buckets -n "${NAMESPACE}" --ignore-not-found
helm uninstall rustfs -n "${NAMESPACE}"
