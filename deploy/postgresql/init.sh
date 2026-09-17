#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

if ! command -v psql >/dev/null 2>&1; then
  echo "ERROR: psql is required; install the PostgreSQL client or run deploy_minikube.sh first." >&2
  exit 1
fi

# refer to https://cloudnative-pg.io/documentation/current/installation_upgrade/
kubectl apply --server-side -f \
  https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.27/releases/cnpg-1.27.1.yaml
