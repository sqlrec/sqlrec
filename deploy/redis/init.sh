#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

# refer to https://valkey.io/valkey-helm/
helm repo add valkey https://valkey.io/valkey-helm/ --force-update
