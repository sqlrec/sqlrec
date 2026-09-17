#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

# refer to https://docs.opensearch.org.cn/docs/latest/install-and-configure/install-opensearch/helm/
helm repo add opensearch https://opensearch-project.github.io/helm-charts/ --force-update
