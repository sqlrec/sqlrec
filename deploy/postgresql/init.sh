#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))

command -v psql >/dev/null 2>&1 || sudo apt-get install -y postgresql-client

# refer to https://cloudnative-pg.io/documentation/current/installation_upgrade/
kubectl apply --server-side -f \
  https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.27/releases/cnpg-1.27.1.yaml