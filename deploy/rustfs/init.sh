#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

# refer to https://github.com/rustfs/rustfs/tree/main/helm/rustfs
helm repo add rustfs https://charts.rustfs.com/ --force-update
