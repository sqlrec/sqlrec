#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ --force-update
