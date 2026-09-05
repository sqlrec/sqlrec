#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"

if ! command -v psql >/dev/null 2>&1; then
  if [ "${DEPLOY_OS}" = darwin ]; then
    echo "ERROR: psql is required. Run 'brew install libpq' and ensure its bin directory is on PATH." >&2
    exit 1
  fi
  sudo apt-get install -y postgresql-client
fi

# refer to https://cloudnative-pg.io/documentation/current/installation_upgrade/
kubectl apply --server-side -f \
  https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.27/releases/cnpg-1.27.1.yaml