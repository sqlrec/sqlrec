#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
