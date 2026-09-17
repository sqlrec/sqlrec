#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

# refer to https://github.com/minio/minio/blob/master/helm/minio/README.md
helm repo add minio https://charts.min.io/ --force-update
